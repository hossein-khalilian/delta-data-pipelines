def fetcher_function(messages):
    if not messages:
        print("No messages available from Sensor.")
        return []

    fetched_data = []
    fetched_data = [{"content_url": msg.get("content_url"), "data": msg} for msg in messages]
    
    print(f"✅Sheypoor fetcher: Passed through {len(fetched_data)} items")
    return fetched_data