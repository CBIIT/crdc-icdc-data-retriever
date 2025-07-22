from datetime import datetime, timezone


def build_notification_message(success: bool, mappings: list, project: str) -> str:
    """Constructs SNS notification message summarizing data retrieval result.

    Args:
        success (bool): Data retrieval and mapping success/failure.
        mappings (list): List of external data mappings associated with project.
        project (str): Name of project being processed.

    Returns:
        str: Formatted message for SNS notification.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    header = f"{'✅' if success else '❌'} Data Retriever Service Pipeline Report for '{project}'"
    time_info = f"Time: {timestamp}"

    if success:
        num_entities = len(mappings)
        num_links = sum(len(mapping["CRDCLinks"]) for mapping in mappings)
        body = (
            f"🎯 Success: Found CRDC dataset links for {num_entities} entities in project '{project}'.\n"
            f"🔗 Total CRDC Links Retrieved: {num_links}"
        )
    else:
        body = f"🚨 The pipeline encountered an error. Please check the logs for more details."

    return f"{header}\n{time_info}\n\n{body}"
