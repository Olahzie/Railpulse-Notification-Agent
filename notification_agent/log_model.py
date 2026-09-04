"""Log and register the rail-notification LangGraph agent to Unity Catalog.

Run this once (or whenever agent.py changes) before deploy_agent.py. Needs the
`notification-agent` optional dependency group installed:

    uv sync --extra notification-agent
    uv run python src/pulse_rail/notification_agent/log_model.py
"""

import os

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
AGENT_PATH = os.path.join(_THIS_DIR, "agent.py")

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksSQLWarehouse


from notification_agent.agent import (
    AGENT,
    GET_MATCHING_PREFERENCES_FN,
    GET_ALERT_HISTORY_FN,
    LLM_ENDPOINT,
    WAREHOUSE_ID,
)

CATALOG = os.environ.get("PULSE_CATALOG", "bootcamp_students")
SCHEMA = os.environ.get("PULSE_SCHEMA", "pulse")
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.rail_notification_agent"

mlflow.set_registry_uri("databricks-uc")

if not WAREHOUSE_ID:
    raise SystemExit("PULSE_WAREHOUSE_ID is not set — cannot declare the SQL warehouse resource.")

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksFunction(function_name=GET_MATCHING_PREFERENCES_FN),
    DatabricksFunction(function_name=GET_ALERT_HISTORY_FN),
    DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID),
]
# `record_notifications` is a plain custom tool (not a UC function/resource) -- it
# authenticates via the serving endpoint's own WorkspaceClient() at call time instead.

input_example = {
    "input": [
        {
            "role": "user",
            "content": (
                "event_id=evt-123 route=London-Birmingham station=Euston "
                "delay_minutes=25 event_date=2026-07-28 event_type=DEPARTURE "
                "is_delayed=true is_cancelled=false"
            ),
        }
    ]
}

print("Starting MLflow logging...")

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=AGENT_PATH,
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            "mlflow==3.6.0",
            "databricks-langchain",
            "langgraph==0.3.4",
            "pydantic",
            "requests",
        ],
    )
    print("Model logged successfully")
    print(f"Model URI: {model_info.model_uri}")

uc_model_info = mlflow.register_model(model_uri=model_info.model_uri, name=REGISTERED_MODEL_NAME)
print("Model registered successfully")
print(uc_model_info.name)
print(uc_model_info.version)

print(f"Registered: {uc_model_info.name} version {uc_model_info.version}")
print(f"NOTE: UC function tools used = {GET_MATCHING_PREFERENCES_FN}, {GET_ALERT_HISTORY_FN}")
