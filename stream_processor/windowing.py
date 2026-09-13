# StreamForge - Week 3
# 5-Minute Tumbling Window

WINDOW_SIZE_SECONDS = 300


def get_window_start(event_timestamp):
    """
    Calculate the start of the 5-minute tumbling window
    using the event timestamp.
    """
    return (event_timestamp // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS


def update_window(state, event_timestamp, value):
    """
    Add an event to its corresponding 5-minute window
    and calculate the updated average.
    """

    window_start = get_window_start(event_timestamp)
    window_key = str(window_start)

    # Create a new window if it does not exist
    if window_key not in state:
        state[window_key] = {
            "window_start": window_start,
            "count": 0,
            "sum": 0.0,
            "average": 0.0
        }

    # Update window statistics
    state[window_key]["count"] += 1
    state[window_key]["sum"] += float(value)

    # Calculate rolling average
    state[window_key]["average"] = (
        state[window_key]["sum"] /
        state[window_key]["count"]
    )

    return state[window_key]