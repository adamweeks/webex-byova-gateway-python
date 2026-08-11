import subprocess
import tarfile
from pathlib import Path, PurePosixPath

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build-runtime-release.sh"
RUNTIME_REQUIREMENTS = REPOSITORY_ROOT / "requirements.txt"
DEVELOPER_REQUIREMENTS = REPOSITORY_ROOT / "requirements-dev.txt"
AUDIO_LAB_REQUIREMENTS = {
    "aiohttp",
    "aiohappyeyeballs",
    "aiosignal",
    "attrs",
    "frozenlist",
    "multidict",
    "yarl",
    "idna",
    "propcache",
}
REQUIRED_MEMBERS = {
    "main.py",
    "requirements.txt",
    "src/generated/byova_common_pb2.py",
    "src/generated/byova_common_pb2_grpc.py",
    "src/generated/health_pb2.py",
    "src/generated/health_pb2_grpc.py",
    "src/generated/voicevirtualagent_pb2.py",
    "src/generated/voicevirtualagent_pb2_grpc.py",
}
FORBIDDEN_DIRECTORIES = {"docs", "tests", "tools"}
FORBIDDEN_FILENAMES = {
    "requirements-dev.txt",
    "package.json",
    "package-lock.json",
    "npm-shrinkwrap.json",
    "yarn.lock",
    "pnpm-lock.yaml",
}


def test_audio_lab_web_server_dependency_is_developer_only() -> None:
    runtime_requirements = RUNTIME_REQUIREMENTS.read_text(encoding="utf-8")
    developer_requirements = DEVELOPER_REQUIREMENTS.read_text(encoding="utf-8")

    assert "-r requirements.txt" in developer_requirements
    for requirement in AUDIO_LAB_REQUIREMENTS:
        assert requirement not in runtime_requirements
        assert requirement in developer_requirements


def test_runtime_release_contains_only_runtime_files(tmp_path: Path) -> None:
    archive_path = tmp_path / "byova-gateway-runtime.tar.gz"

    subprocess.run(
        [str(BUILD_SCRIPT), "--ref", "HEAD", "--output", str(archive_path)],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    with tarfile.open(archive_path, "r:gz") as archive:
        members = {member.name for member in archive.getmembers()}

    assert REQUIRED_MEMBERS <= members
    assert any(name.startswith("config/") for name in members)
    assert any(name.startswith("proto/") for name in members)
    assert any(name.startswith("src/") for name in members)

    for member in members:
        path = PurePosixPath(member)
        assert not (FORBIDDEN_DIRECTORIES & set(path.parts))
        assert path.name not in FORBIDDEN_FILENAMES
        assert not any(part.startswith("._") for part in path.parts)
