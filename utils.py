from sys import executable
from subprocess import check_call, CalledProcessError


def install_package(package, version="upgrade"):
    result = False
    if version.lower() == "upgrade":
        try:
            result = check_call([executable, "-m", "pip", "install", package, "--upgrade", "--user"])
        except CalledProcessError:
            result = check_call([executable, "-m", "pip", "install", package, "--upgrade"])
    else:
        from pkg_resources import get_distribution
        current_package_version = None
        try:
            current_package_version = get_distribution(package)
        except Exception:
            pass
        if current_package_version is None or current_package_version != version:
            installation_sign = "==" if ">=" not in version else ""
            try:
                result = check_call(
                    [executable, "-m", "pip", "install", package + installation_sign + version, "--user"])
            except CalledProcessError:
                result = check_call([executable, "-m", "pip", "install", package + installation_sign + version])
    return result