"""OCI distribution-spec 风格的错误响应。

错误体格式：{"errors": [{"code": ..., "message": ..., "detail": ...}]}
"""
from __future__ import annotations


class OCIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, detail=None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail

    def body(self) -> dict:
        err = {"code": self.code, "message": self.message}
        if self.detail is not None:
            err["detail"] = self.detail
        return {"errors": [err]}


def name_invalid(repo: str) -> OCIError:
    return OCIError(400, "NAME_INVALID", f"invalid repository name: {repo!r}",
                    {"repo": repo})


def digest_invalid(digest: str) -> OCIError:
    return OCIError(400, "DIGEST_INVALID", f"invalid digest: {digest!r}",
                    {"digest": digest})


def digest_mismatch(expected: str, computed: str) -> OCIError:
    return OCIError(400, "DIGEST_INVALID",
                    f"content digest mismatch: expected {expected}, computed {computed}",
                    {"expected": expected, "computed": computed})


def blob_unknown(repo: str, digest: str) -> OCIError:
    return OCIError(404, "BLOB_UNKNOWN",
                    f"blob {digest} unknown in repo '{repo}'",
                    {"repo": repo, "digest": digest})


def upload_unknown(upload_id: str) -> OCIError:
    return OCIError(404, "BLOB_UPLOAD_UNKNOWN",
                    f"blob upload unknown, expired or cancelled: {upload_id}",
                    {"upload_id": upload_id})


def upload_invalid(message: str, detail=None) -> OCIError:
    return OCIError(400, "BLOB_UPLOAD_INVALID", message, detail)


def range_invalid(expected_start: int, content_range: str) -> OCIError:
    return OCIError(416, "RANGE_INVALID",
                    f"chunk starts at wrong offset: expected {expected_start}, "
                    f"got Content-Range {content_range!r}",
                    {"expected_start": expected_start, "content_range": content_range})


def manifest_unknown(repo: str, ref: str) -> OCIError:
    return OCIError(404, "MANIFEST_UNKNOWN",
                    f"manifest unknown: {repo}:{ref}",
                    {"repo": repo, "ref": ref})


def manifest_invalid(message: str) -> OCIError:
    return OCIError(400, "MANIFEST_INVALID", f"manifest invalid: {message}")


def manifest_blob_unknown(repo: str, missing: list) -> OCIError:
    return OCIError(400, "MANIFEST_BLOB_UNKNOWN",
                    f"manifest references unknown content in repo '{repo}': "
                    + ", ".join(missing),
                    {"repo": repo, "missing": missing})


def tag_invalid(ref: str) -> OCIError:
    return OCIError(400, "TAG_INVALID", f"invalid tag or reference: {ref!r}",
                    {"ref": ref})


def tag_conflict(repo: str, tag: str, current: "str | None", expected: "str | None") -> OCIError:
    return OCIError(
        409, "TAG_CONFLICT",
        f"tag '{tag}' in repo '{repo}' currently points to "
        f"{current or '<absent>'}, not {expected or '<unspecified>'}; "
        f"re-send with header 'If-Match: <current digest>' to move the tag",
        {"repo": repo, "tag": tag, "current": current, "expected": expected},
    )
