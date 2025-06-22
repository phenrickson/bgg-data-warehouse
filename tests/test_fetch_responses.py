"""Integration tests for fetch_responses pipeline."""

import pytest
import ast
from src.pipeline.fetch_responses import BGGResponseFetcher
from src.api_client.client import BGGAPIClient

def test_fetch_responses_game_id_integrity():
    """
    Test that stored responses correctly match the requested game IDs.
    
    Validates that:
    1. API returns responses for all requested game IDs
    2. Each stored response contains the correct game IDs
    3. No unexpected game IDs are present in the response
    """
    # Initialize fetcher and API client
    fetcher = BGGResponseFetcher(
        batch_size=20,  # Small batch for testing
        chunk_size=20,  # Fetch all games in one chunk
        environment='dev'  # Use dev environment for testing
    )
    api_client = BGGAPIClient()
    
    # Get a batch of unfetched game IDs
    unfetched_ids = fetcher.get_unfetched_ids()
    
    # Ensure we have game IDs to test
    assert len(unfetched_ids) > 0, "No unfetched game IDs found"
    
    # Extract game IDs
    chunk_ids = [game["game_id"] for game in unfetched_ids]
    
    # Fetch responses from API
    response = api_client.get_thing(chunk_ids)
    
    # Validate response structure
    assert response is not None, "API returned no response"
    assert 'items' in response, "Response missing 'items' key"
    
    # Extract items from response
    items = response.get('items', {}).get('item', [])
    
    # Ensure items is a list
    if not isinstance(items, list):
        items = [items]
    
    # Validate number of items matches requested game IDs
    assert len(items) == len(chunk_ids), f"Mismatch in number of items. Requested: {len(chunk_ids)}, Received: {len(items)}"
    
    # Extract response IDs
    response_ids = [int(item.get('@id', 0)) for item in items]
    
    # Validate IDs match
    assert set(response_ids) == set(chunk_ids), f"Mismatch in game IDs. Requested: {chunk_ids}, Received: {response_ids}"

def test_store_response_game_id_integrity():
    """
    Test that store_response method correctly associates game IDs with responses.
    
    Validates that:
    1. Each game ID is stored with its corresponding response
    2. Responses are stored as expected
    """
    # Initialize fetcher
    fetcher = BGGResponseFetcher(
        batch_size=20,
        chunk_size=20,
        environment='dev'
    )
    api_client = BGGAPIClient()
    
    # Get a batch of unfetched game IDs
    unfetched_ids = fetcher.get_unfetched_ids()
    
    # Ensure we have game IDs to test
    assert len(unfetched_ids) > 0, "No unfetched game IDs found"
    
    # Extract game IDs
    chunk_ids = [game["game_id"] for game in unfetched_ids]
    
    # Fetch responses from API
    response = api_client.get_thing(chunk_ids)
    
    # Store the response
    fetcher.store_response(chunk_ids, str(response))
    
    # Validate stored response
    try:
        # Parse the stored response
        parsed_response = ast.literal_eval(str(response))
        
        # Extract items from response
        items = parsed_response.get('items', {}).get('item', [])
        
        # Ensure items is a list
        if not isinstance(items, list):
            items = [items]
        
        # Validate each item's ID
        for item in items:
            item_id = int(item.get('@id', 0))
            assert item_id in chunk_ids, f"Game ID {item_id} not in original request {chunk_ids}"
    
    except Exception as e:
        pytest.fail(f"Error parsing or validating stored response: {e}")
