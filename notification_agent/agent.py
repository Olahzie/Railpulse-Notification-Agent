"""LangGraph rail-notification agent.

Given one ai_alert row, this agent:
  1. Calls the get_matching_preferences UC function (queries Supabase rider preferences live
     via its REST API) to find which riders should hear about this event.
  2. Optionally calls get_alert_history for recent-pattern context.
  3. Drafts a subject + message per matching rider.
  4. Calls record_notifications exactly once, which emails each rider via Resend, logs the
     outcome to rider_notification_outbox, and marks the ai_alert row as processed -- this is
     what prevents re-notifying on the same event.

Logged/deployed via log_model.py and deploy_agent.py in this same package.
"""

import os
import uuid
from typing import Annotated, Any, Generator, List, Sequence, TypedDict

import mlflow
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from pydantic import BaseModel

CATALOG = os.environ.get("PULSE_CATALOG", "bootcamp_students")
SCHEMA = os.environ.get("PULSE_SCHEMA", "pulse")
# SQL warehouse used only by the record_notifications write tool (Statement Execution API).
WAREHOUSE_ID = os.environ.get("PULSE_WAREHOUSE_ID")
LLM_ENDPOINT = os.environ.get("PULSE_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

GET_MATCHING_PREFERENCES_FN = f"{CATALOG}.{SCHEMA}.get_matching_preferences"
GET_ALERT_HISTORY_FN = f"{CATALOG}.{SCHEMA}.get_alert_history"
AI_ALERT_TABLE = f"{CATALOG}.{SCHEMA}.ai_alert"
OUTBOX_TABLE = f"{CATALOG}.{SCHEMA}.rider_notification_outbox"
HISTORY_LOOKBACK_DAYS = int(os.environ.get("PULSE_HISTORY_LOOKBACK_DAYS", "7"))

# Email delivery (Resend). `email` comes straight from the Supabase preferences row.
RESEND_API_URL = "https://api.resend.com/emails"
RESEND_API_KEY = os.environ.get("PULSE_RESEND_API_KEY")
EMAIL_FROM = os.environ.get("PULSE_EMAIL_FROM", "notifications@railpulse.appotg.com")

SYSTEM_PROMPT = (
    "You are the rail notification agent. You are given one train event/alert "
    "(event_id, route, station, delay_minutes, event_date, current weather conditions "
    "including a plain-language weather description, forecast weather conditions including a "
    "forecast description, and other event details). Follow these steps in order:\n"
    "1. Call get_matching_preferences with the alert's route, station, delay_minutes, "
    "is_delayed, and is_cancelled to find which riders' saved preferences match this event -- "
    "the function already applies the notify_delay/notify_cancellation and delay-threshold "
    "matching based on these flags, do not re-derive or second-guess its logic.\n"
    "2. If at least one rider matched, call get_alert_history with the alert's route, station, "
    f"event_date, and lookback_days={HISTORY_LOOKBACK_DAYS} for extra context only -- never to "
    "decide whether to notify.\n"
    "3. For each rider returned, draft a subject line and message body. The subject line should "
    "be short and specific, e.g. 'Delay at Dover Priory: 15 min' or 'Cancellation on Kent route "
    "at Dover Priory' -- not generic like 'Train Update'.\n"
    "\n"
    "For the message body, follow this structure:\n"
    "   Sentence 1 (mandatory, always first): State plainly what happened -- the route, "
    "station, and whether it was a delay (with length), cancellation, or major delay.\n"
    "   Weather mention (only include if current_weather_description indicates something that "
    "could plausibly disrupt rail travel -- rain, snow, storm, fog, ice, high wind, extreme "
    "heat/cold. Do NOT mention weather at all if the description is mild/non-disruptive, e.g. "
    "'clear sky', 'few clouds', 'scattered clouds', 'broken clouds' -- move straight to the next "
    "part instead. When you do mention it, state it as a plausible link in natural language, "
    "e.g. 'this may be linked to heavy rain in the area' -- never as certain fact, and never by "
    "restating raw numbers.\n"
    "   Forecast mention (only include if forecast_weather_description differs meaningfully from "
    "current_weather_description, or if you mentioned a disruptive current condition and the "
    "forecast confirms it will persist/worsen/improve. Before stating a direction, carefully "
    "compare the actual severity of the two descriptions -- do not assume any change is a "
    "worsening just because the wording changed. Rough order of increasing severity: "
    "clear/sunny < few/scattered clouds < broken/overcast clouds < mist/fog < light rain or "
    "light snow < moderate rain or moderate snow < heavy rain or heavy snow < "
    "thunderstorm/severe. If the forecast description is LOWER severity than the current one, "
    "say conditions are expected to EASE or IMPROVE. If it is HIGHER severity, say conditions "
    "are expected to WORSEN. If similar severity, say conditions are expected to PERSIST or "
    "CONTINUE. Double-check this direction carefully before writing it -- getting it backwards "
    "is a serious error riders would notice. State the practical implication, e.g. 'similar "
    "conditions are expected through the evening' or 'conditions should clear up soon'. Skip "
    "this entirely if it adds nothing new.\n"
    "   Pattern mention (only if get_alert_history shows a genuine recent pattern on this route/"
    "station): one sentence noting it.\n"
    "   Never include raw numeric weather values (temperature, wind speed, pressure, humidity, "
    "etc.) anywhere in the message.\n"
    "\n"
    "Examples of good messages:\n"
    "- Mild delay, non-disruptive weather: 'The 14:18 arrival at Peterborough on the East Coast "
    "route was delayed by 10 minutes. The cause is unclear.'\n"
    "- Delay with disruptive weather easing: 'The train on the Wessex route was delayed by 26 "
    "minutes at Portsmouth Harbour. This may be linked to heavy rain in the area -- conditions "
    "are expected to ease through the afternoon.'\n"
    "- Delay with disruptive weather worsening: 'The train on the Kent route was delayed by 20 "
    "minutes at Dover Priory. This may be linked to rain in the area, which is forecast to "
    "intensify -- plan for possible further delays.'\n"
    "- Cancellation, no weather link: 'Your train on the Sussex route via Norwood Junction has "
    "been cancelled. The cause is unclear at this time.'\n"
    "Examples of what NOT to do:\n"
    "- Do not write: 'The temperature is 21.74C and wind speed is 3.58 km/h.' (raw numbers, no "
    "interpretation)\n"
    "- Do not write: 'Current weather is broken clouds.' when clouds are not disruptive "
    "(mentions weather with nothing useful to say)\n"
    "- Do not say conditions are 'worsening' when the forecast description is actually less "
    "severe than the current one, or vice versa -- always verify the severity direction first.\n"
    "\n"
    "4. Call record_notifications exactly once with the event_id and every drafted "
    "notification -- pass an empty drafts list if no riders matched. This must always happen, "
    "even when there is nothing to notify."
)


class NotificationDraft(BaseModel):
    user_id: str
    email: str
    subject: str
    message: str


def _send_email(to: str, subject: str, body: str) -> bool:
    """Send one email via Resend. Returns True on success, False on any failure (never raises)."""
    if not RESEND_API_KEY:
        print("PULSE_RESEND_API_KEY is not configured; skipping email send.")
        return False
    try:
        response = requests.post(
            RESEND_API_URL,
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={"from": EMAIL_FROM, "to": [to], "subject": subject, "text": body},
            timeout=10,
        )
        if response.status_code >= 400:
            print(f"Resend send to {to} failed ({response.status_code}): {response.text}")
            return False
        return True
    except requests.RequestException as e:
        print(f"Resend send to {to} raised: {e}")
        return False


@tool
def record_notifications(event_id: str, drafts: List[NotificationDraft]) -> str:
    """Email each drafted rider notification, log the outcome, and mark the event as processed.

    Call this exactly once per event, after drafting a subject+message for every rider returned
    by get_matching_preferences (pass an empty `drafts` list if nobody matched). This is a plain
    tool rather than a UC function because it performs side effects (sending email, INSERT,
    UPDATE); UC SQL functions are read-only table functions and can't do that.
    """
    if not WAREHOUSE_ID:
        return "Error: PULSE_WAREHOUSE_ID is not configured for this agent."

    w = WorkspaceClient()
    sent_count = 0
    failed_count = 0

    for draft in drafts:
        delivered = _send_email(draft.email, draft.subject, draft.message)
        delivery_status = "SENT" if delivered else "FAILED"
        sent_count += 1 if delivered else 0
        failed_count += 0 if delivered else 1

        insert_result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=(
                f"INSERT INTO {OUTBOX_TABLE} "
                "(notification_id, event_id, user_id, email, route, station, subject, "
                "message, delivery_status, created_at) "
                "VALUES (:notification_id, :event_id, :user_id, :email, NULL, NULL, "
                ":subject, :message, :delivery_status, current_timestamp())"
            ),
            parameters=[
                StatementParameterListItem(name="notification_id", value=str(uuid.uuid4()), type="STRING"),
                StatementParameterListItem(name="event_id", value=event_id, type="STRING"),
                StatementParameterListItem(name="user_id", value=draft.user_id, type="STRING"),
                StatementParameterListItem(name="email", value=draft.email, type="STRING"),
                StatementParameterListItem(name="subject", value=draft.subject, type="STRING"),
                StatementParameterListItem(name="message", value=draft.message, type="STRING"),
                StatementParameterListItem(name="delivery_status", value=delivery_status, type="STRING"),
            ],
            wait_timeout="30s",
        )
        if insert_result.status.state.value != "SUCCEEDED":
            print(f"INSERT into outbox failed for event_id={event_id!r}: {insert_result.status.error}")

    update_result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=(
            f"UPDATE {AI_ALERT_TABLE} SET notification_status = 'SENT', "
            "notified_at = current_timestamp() WHERE event_id = :event_id"
        ),
        parameters=[StatementParameterListItem(name="event_id", value=event_id, type="STRING")],
        wait_timeout="30s",
    )
    if update_result.status.state.value != "SUCCEEDED":
        error_detail = f"UPDATE of ai_alert FAILED for event_id={event_id!r}: {update_result.status.error}"
        print(error_detail)
        return f"Emailed {sent_count} notification(s), {failed_count} failed, for event {event_id}. WARNING: {error_detail}"

    return f"Emailed {sent_count} notification(s), {failed_count} failed, for event {event_id}."


uc_toolkit = UCFunctionToolkit(function_names=[GET_MATCHING_PREFERENCES_FN, GET_ALERT_HISTORY_FN])
tools = list(uc_toolkit.tools) + [record_notifications]

llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.5)
llm_with_tools = llm.bind_tools(tools)


class AgentState(TypedDict):
    messages: Annotated[Sequence[Any], add_messages]


class NotificationAgent(ResponsesAgent):
    def __init__(self):
        self.llm_with_tools = llm_with_tools
        self.tools = tools

    def _build_graph(self):
        def should_continue(state):
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "tools"
            return "end"

        def call_model(state):
            messages = [{"role": "system", "content": SYSTEM_PROMPT}] + list(state["messages"])
            response = self.llm_with_tools.invoke(messages)
            return {"messages": [response]}

        graph = StateGraph(AgentState)
        graph.add_node("agent", RunnableLambda(call_model))
        graph.add_node("tools", ToolNode(self.tools))
        graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
        graph.add_edge("tools", "agent")
        graph.set_entry_point("agent")
        return graph.compile()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        graph = self._build_graph()
        for event in graph.stream({"messages": messages}, stream_mode=["updates"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    if node_data.get("messages"):
                        yield from output_to_responses_items_stream(node_data["messages"])


mlflow.langchain.autolog()
AGENT = NotificationAgent()
mlflow.models.set_model(AGENT)
