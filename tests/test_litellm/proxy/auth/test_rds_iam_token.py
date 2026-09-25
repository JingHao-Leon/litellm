from typing import Final
from unittest.mock import MagicMock

import pytest

from litellm.proxy.auth.rds_iam_token import generate_iam_auth_token


@pytest.fixture()
def boto3_calls(monkeypatch: pytest.MonkeyPatch) -> list:
    calls: Final = []

    def fake_client(service_name, **kwargs):
        client = MagicMock()
        client.assume_role.return_value = {
            "Credentials": {"AccessKeyId": "AKIA", "SecretAccessKey": "s", "SessionToken": "t"}
        }
        client.assume_role_with_web_identity.return_value = {
            "Credentials": {"AccessKeyId": "AKIA", "SecretAccessKey": "s", "SessionToken": "t"}
        }
        client.generate_db_auth_token.return_value = "tok"
        calls.append({"service_name": service_name, "kwargs": kwargs, "client": client})
        return client

    monkeypatch.setattr("boto3.client", fake_client)
    return calls


@pytest.fixture()
def eks_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> str:
    token_file = tmp_path / "token"
    token_file.write_text("oidc")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "EXPLICITKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "explicitsecret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "explicittoken")
    monkeypatch.setenv("AWS_ROLE_NAME", "arn:aws:iam::123:role/rds-role")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123:role/eks-role")
    monkeypatch.setenv("AWS_SESSION_NAME", "litellm-session")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))
    return str(token_file)


def sts_calls(calls: list) -> list:
    return [c for c in calls if c["service_name"] == "sts"]


def rds_calls(calls: list) -> list:
    return [c for c in calls if c["service_name"] == "rds"]


def test_flag_set_skips_web_identity_and_assumes_role_with_explicit_creds(
    monkeypatch: pytest.MonkeyPatch, boto3_calls: list, eks_env: str
) -> None:
    monkeypatch.setenv("AWS_RDS_IAM_IGNORE_WEB_IDENTITY_TOKEN", "true")

    generate_iam_auth_token("h", 5432, "u")

    sts = sts_calls(boto3_calls)
    assert len(sts) == 1, f"boto3.client calls: {boto3_calls}"
    assert sts[0]["kwargs"] == {
        "aws_access_key_id": "EXPLICITKEY",
        "aws_secret_access_key": "explicitsecret",
        "aws_session_token": "explicittoken",
    }, f"boto3.client calls: {boto3_calls}"
    sts[0]["client"].assume_role_with_web_identity.assert_not_called()
    sts[0]["client"].assume_role.assert_called_once_with(
        RoleArn="arn:aws:iam::123:role/rds-role", RoleSessionName="litellm-session"
    )


def test_flag_set_without_role_uses_explicit_keys_with_session_token(
    monkeypatch: pytest.MonkeyPatch, boto3_calls: list, eks_env: str
) -> None:
    monkeypatch.setenv("AWS_RDS_IAM_IGNORE_WEB_IDENTITY_TOKEN", "true")
    monkeypatch.delenv("AWS_ROLE_NAME")
    monkeypatch.delenv("AWS_ROLE_ARN")

    generate_iam_auth_token("h", 5432, "u")

    assert sts_calls(boto3_calls) == [], f"boto3.client calls: {boto3_calls}"
    rds = rds_calls(boto3_calls)
    assert len(rds) == 1, f"boto3.client calls: {boto3_calls}"
    kwargs = rds[0]["kwargs"]
    assert kwargs == {
        "aws_access_key_id": "EXPLICITKEY",
        "aws_secret_access_key": "explicitsecret",
        "aws_session_token": "explicittoken",
        "region_name": "us-east-1",
        "config": kwargs["config"],
    }, f"boto3.client calls: {boto3_calls}"


def test_flag_unset_keeps_web_identity_branch(boto3_calls: list, eks_env: str) -> None:
    generate_iam_auth_token("h", 5432, "u")

    sts = sts_calls(boto3_calls)
    assert len(sts) == 1, f"boto3.client calls: {boto3_calls}"
    sts[0]["client"].assume_role.assert_not_called()
    sts[0]["client"].assume_role_with_web_identity.assert_called_once_with(
        RoleArn="arn:aws:iam::123:role/rds-role",
        RoleSessionName="litellm-session",
        WebIdentityToken="oidc",
        DurationSeconds=3600,
    )
