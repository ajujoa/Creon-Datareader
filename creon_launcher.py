# coding=utf-8
"""
Creon Plus 자동 실행 및 로그인 모듈
- 이미 실행 중인지 확인
- 미실행 시 coStarter.exe /prj:cp 실행 후 로그인 다이얼로그 직접 입력
- ID/PW는 creon_config.ini에서 읽음
- 로그인 성공 시 CpCybos 연결 상태 확인
"""

import os
import sys
import time
import ctypes
import configparser
import logging
from pathlib import Path

import win32com.client
import win32gui
import win32con
import psutil

logger = logging.getLogger('creon_chart')

CONFIG_PATH = Path(__file__).parent / "creon_config.ini"

# Creon Plus 강제 종료 대상 프로세스 (terminate_creon에서 사용)
CREON_KILL_NAMES = {
    "costarter.exe", "cybosstarter.exe", "cpstart.exe",
    "ncfsys.exe", "dicagyw.exe",
}


def load_config(config_path: Path = None) -> configparser.ConfigParser:
    path = config_path or CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {path}")
    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")
    return config


def is_creon_running() -> bool:
    """Creon Plus 관련 프로세스 실행 여부 확인"""
    target = {"costarter.exe", "cybosstarter.exe", "ncfsys.exe", "dicagyw.exe"}
    for proc in psutil.process_iter(["name"]):
        try:
            if proc.info["name"] and proc.info["name"].lower() in target:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False


def terminate_creon() -> None:
    """Creon Plus 프로세스 강제 종료 (프로그램 종료 직전 호출)

    is_creon_running()과 동일한 psutil 순회 패턴. 종료 시에는
    CpStart/DsCbo1 등 보조 프로세스까지 전부 종료한다.
    """
    killed = []
    failed = []
    for proc in psutil.process_iter(["name"]):
        try:
            name = (proc.info["name"] or "").lower()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if name in CREON_KILL_NAMES or name.startswith("dscbo"):
            try:
                proc.kill()
                killed.append(name)
            except psutil.AccessDenied:
                failed.append(name)
            except psutil.NoSuchProcess:
                pass
    if killed:
        logger.info(f"Creon 프로세스 종료: {', '.join(killed)}")
    if failed:
        logger.warning(f"Creon 프로세스 종료 실패(권한 부족): {', '.join(failed)}")


def is_cybos_connected() -> bool:
    """CYBOS/CpCybos API 연결 상태 확인"""
    try:
        obj = win32com.client.Dispatch("CpUtil.CpCybos")
        return obj.IsConnect == 1
    except Exception:
        return False


def _fill_login_dialog(creon_id: str, creon_pw: str, login_timeout: int) -> bool:
    """로그인 다이얼로그 찾아서 ID/PW 직접 입력"""
    start = time.time()
    hwnd = None
    while time.time() - start < login_timeout:
        # 1순위: CREON Starter 다이얼로그 (가장 정확)
        hwnd = win32gui.FindWindow("#32770", "CREON Starter")
        if hwnd:
            break
        # 2순위: Edit 컨트롤이 있는 #32770
        def find_login(h, _):
            nonlocal hwnd
            if win32gui.GetClassName(h) == "#32770":
                if win32gui.FindWindowEx(h, None, "Edit", None):
                    text = win32gui.GetWindowText(h)
                    if "CREON" in text or text == "":  # CREON 관련 다이얼로그
                        hwnd = h
                        return False
            return True
        win32gui.EnumWindows(find_login, None)
        if hwnd:
            break
        time.sleep(0.5)

    if not hwnd:
        logger.error("로그인 다이얼로그를 찾을 수 없음")
        def dump_top(h, _):
            try:
                cls = win32gui.GetClassName(h)
                txt = win32gui.GetWindowText(h)
                logger.info(f"  top-window: {cls} text='{txt}'")
            except:
                pass
            return True
        logger.info("--- top-level window dump ---")
        win32gui.EnumWindows(dump_top, None)
        logger.info("--- end dump ---")
        return False

    logger.info(f"로그인 다이얼로그 찾음: class={win32gui.GetClassName(hwnd)} text='{win32gui.GetWindowText(hwnd)}'")

    # dump dialog children
    def dump_child(h, _):
        try:
            cls = win32gui.GetClassName(h)
            txt = win32gui.GetWindowText(h)
            cid = win32gui.GetDlgCtrlID(h)
            rect = win32gui.GetWindowRect(h)
            style = win32gui.GetWindowLong(h, win32con.GWL_STYLE)
            enabled = (style & win32con.WS_DISABLED) == 0
            visible = (style & win32con.WS_VISIBLE) != 0
            logger.info(f"  child: {cls} id={cid} text='{txt}' enabled={enabled} visible={visible} rect={rect}")
        except:
            pass
        return True
    logger.info("--- dialog children ---")
    win32gui.EnumChildWindows(hwnd, dump_child, None)
    logger.info("--- end children ---")

    try:
        win32gui.SetForegroundWindow(hwnd)
    except:
        pass
    time.sleep(0.3)

    # Edit 컨트롤을 ID로 직접 찾기 (업데이트 후 3개 Edit 존재: 6977=ID, 6978=PW, 6979=?)
    EDIT_IDS = [6977, 6978, 6979]
    edits = {}
    def find_edits(h, _):
        cid = win32gui.GetDlgCtrlID(h)
        if cid in EDIT_IDS:
            edits[cid] = h
        return True
    win32gui.EnumChildWindows(hwnd, find_edits, None)

    # ponytail: 3개 Edit — 6977=ID, 6978=PW (6979는 추가필드로 추정)
    edit_id = edits.get(6977)
    edit_pw = edits.get(6978)

    try:
        win32gui.SetForegroundWindow(hwnd)
    except:
        pass
    time.sleep(0.1)

    logger.info("ID 입력 직전")
    time.sleep(0.1)
    if edit_id:
        # Clear first, then set
        win32gui.SendMessage(edit_id, win32con.EM_SETSEL, 0, -1)
        win32gui.SendMessage(edit_id, win32con.EM_REPLACESEL, 0, creon_id)
        time.sleep(0.1)
    logger.info("ID 입력 후")
    time.sleep(0.1)

    logger.info("PW 입력 직전")
    time.sleep(0.1)
    if edit_pw:
        win32gui.SendMessage(edit_pw, win32con.EM_SETSEL, 0, -1)
        win32gui.SendMessage(edit_pw, win32con.EM_REPLACESEL, 0, creon_pw)
        time.sleep(0.1)
    logger.info("PW 입력 후")
    time.sleep(0.1)

    # ponytail: Edit에 WM_KEYDOWN/UP 대신 Button 찾아서 클릭
    # SSH 세션에서 SendMessage keyboard가 dialog button click으로
    # 연결되지 않는 경우가 많음.
    btn_hwnd = None
    def find_btn(h, _):
        nonlocal btn_hwnd
        try:
            cls = win32gui.GetClassName(h)
            txt = win32gui.GetWindowText(h)
            if cls == "Button" and txt in ("확인", "OK", "로그인", "Login"):
                btn_hwnd = h
                return False
        except:
            pass
        return True
    win32gui.EnumChildWindows(hwnd, find_btn, None)

    if btn_hwnd:
        logger.info(f"로그인 버튼 찾음: text='{win32gui.GetWindowText(btn_hwnd)}'")
        win32gui.SendMessage(btn_hwnd, win32con.BM_CLICK, 0, 0)
    else:
        # fallback: dialog에 WM_COMMAND IDOK 전송
        logger.warning("로그인 버튼 못찾음, WM_COMMAND IDOK fallback")
        win32gui.SendMessage(hwnd, win32con.WM_COMMAND, 1, 0)

    logger.info("로그인 정보 입력 완료")
    return True


def launch_and_login(config_path: Path = None) -> bool:
    """
    Creon Plus 실행 및 로그인
    coStarter.exe /prj:cp 실행 → 다이얼로그 직접 입력
    Returns: True if connected, False otherwise
    """
    config = load_config(config_path)
    creon_id = config["creon"]["id"]
    creon_pw = config["creon"]["password"]
    starter_path = config["launcher"]["starter_path"]
    settle_timeout = int(config["launcher"].get("settle_timeout", 30))
    login_timeout = int(config["launcher"].get("login_timeout", 60))

    # 이미 연결되어 있으면 성공
    if is_cybos_connected():
        logger.info("Creon Plus 이미 연결됨")
        return True

    if not os.path.exists(starter_path):
        raise FileNotFoundError(f"coStarter.exe를 찾을 수 없습니다: {starter_path}")

    # ponytail: 이미 실행 중이면 중복 실행 방지
    if is_creon_running():
        logger.info("Creon Plus 프로세스 이미 실행 중, 실행 생략하고 연결 대기")
        # 연결 대기
        logger.info("Creon Plus 연결 대기 중...")
        for i in range(settle_timeout):
            time.sleep(1)
            if is_cybos_connected():
                logger.info("Creon Plus 연결 성공!")
                return True
        logger.error("Creon Plus 연결 타임아웃")
        return False

    # /prj:cp 만 전달 (id/pw는 직접 입력)
    logger.info(f"Creon Plus 실행: {starter_path} /prj:cp")
    try:
        ret = ctypes.windll.shell32.ShellExecuteW(
            None,
            "runas",
            starter_path,
            "/prj:cp",
            os.path.dirname(starter_path),
            1  # SW_SHOWNORMAL
        )
        if ret <= 32:
            logger.error(f"ShellExecute 실패: {ret}")
            return False
    except Exception as e:
        logger.error(f"Creon Plus 실행 실패: {e}")
        return False

    # 로그인 다이얼로그 직접 입력
    _fill_login_dialog(creon_id, creon_pw, login_timeout)

    # 연결 대기
    logger.info("Creon Plus 연결 대기 중...")
    for i in range(settle_timeout):
        time.sleep(1)
        if is_cybos_connected():
            logger.info("Creon Plus 연결 성공!")
            return True

    logger.error("Creon Plus 연결 타임아웃")
    return False


def ensure_connected(config_path: Path = None) -> bool:
    """Creon Plus 연결 보장"""
    if is_cybos_connected():
        logger.info("Creon Plus 이미 연결됨")
        return True
    return launch_and_login(config_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(levelname)-7s] %(message)s")
    result = ensure_connected()
    print(f"결과: {'성공' if result else '실패'}")
    sys.exit(0 if result else 1)
