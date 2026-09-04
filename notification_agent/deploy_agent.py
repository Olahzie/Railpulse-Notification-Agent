"""Deploy the registered rail-notification agent to a Model Serving endpoint.

Run once after log_model.py has registered a model version. Deployment takes ~15 minutes,
so this is meant to be run as a one-off job/script, not on every trigger.

    uv run python src/pulse_rail/notification_agent/deploy_agent.py <version>
"""

import os
import sys

from databricks import agents

CATALOG = os.environ.get("PULSE_CATALOG", "bootcamp_students")
SCHEMA = os.environ.get("PULSE_SCHEMA", "pulse")
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.rail_notification_agent"
ENDPOINT_NAME = os.environ.get("PULSE_NOTIFICATION_ENDPOINT_NAME", "rail-notification-agent")
SECRET_SCOPE = os.environ.get("PULSE_SECRET_SCOPE", "pulse")
EMAIL_FROM = os.environ.get("PULSE_EMAIL_FROM", "notifications@railpulse.appotg.com")




def main(version=None):
    if version is None:
        if len(sys.argv) < 2:
            raise SystemExit("Usage: deploy_agent.py <model_version>")
        version = sys.argv[1]

    deployment = agents.deploy(
        REGISTERED_MODEL_NAME,
        version,
        endpoint_name=ENDPOINT_NAME,
        environment_vars={
            "PULSE_CATALOG": CATALOG,
            "PULSE_SCHEMA": SCHEMA,
            "PULSE_WAREHOUSE_ID": os.environ.get("PULSE_WAREHOUSE_ID", ""),
            "PULSE_LLM_ENDPOINT": os.environ.get("PULSE_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"),
            "PULSE_EMAIL_FROM": EMAIL_FROM,
            "PULSE_REPLY_TO": os.environ.get("PULSE_REPLY_TO", "notifications@railpulse.appotg.com"),
            "PULSE_RESEND_API_KEY": f"{{{{secrets/{SECRET_SCOPE}/RESEND_API_KEY}}}}",
        },
        tags={"project": "pulse_rail", "component": "notification_agent"},
    )
    print(f"Endpoint name: {deployment.endpoint_name}")
    print(f"Query URL: {deployment.query_endpoint}")


if __name__ == "__main__":
    main()