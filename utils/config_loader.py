import json
import os
import pkgutil
from typing import Dict, Optional


def load_config(s3_path: Optional[str] = None) -> Dict:
    """
    Load configuration for the pipeline.

    Priority order (industry standard):
    1. S3 path (if provided)
    2. Environment variable CONFIG_PATH
    3. Packaged config inside ZIP (fallback)

    Args:
        s3_path (str, optional): S3 path to config file

    Returns:
        dict: Configuration dictionary
    """

    # -----------------------------------
    # 1. Load from S3 (Preferred in prod)
    # -----------------------------------
    if s3_path:
        try:
            import boto3

            s3 = boto3.client("s3")

            bucket, key = _parse_s3_path(s3_path)
            obj = s3.get_object(Bucket=bucket, Key=key)

            return json.loads(obj["Body"].read().decode("utf-8"))

        except Exception as e:
            raise RuntimeError(f"❌ Failed to load config from S3: {s3_path}") from e

    # -----------------------------------
    # 2. Load from ENV path (Optional)
    # -----------------------------------
    env_path = os.getenv("CONFIG_PATH")

    if env_path:
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"❌ CONFIG_PATH not found: {env_path}")

    # -----------------------------------
    # 3. Load from package (ZIP-safe)
    # -----------------------------------
    try:
        data = pkgutil.get_data("BANK_SGD.config", "config.json")

        if data is None:
            raise FileNotFoundError("config.json not found inside package")

        return json.loads(data.decode("utf-8"))

    except Exception as e:
        raise RuntimeError("❌ Failed to load packaged config") from e


# -----------------------------------
# Helper: Parse S3 path
# -----------------------------------
def _parse_s3_path(s3_path: str):
    """
    Parse S3 path into bucket and key

    Example:
        s3://bucket-name/path/to/file.json
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)

    return bucket, key
