# BoardGameGeek XML API2 Documentation

This document outlines the BoardGameGeek XML API2 endpoints and usage guidelines used in this project.

## API Base URL
```
https://boardgamegeek.com/xmlapi2/
```

## Rate Limiting
- Maximum 2 requests per second
- Respect HTTP 429 responses with exponential backoff
- Cache responses when possible

## Endpoints Used

### /thing
Retrieves board game details.

```
GET /xmlapi2/thing?id={game_id}&type=boardgame&stats=1
```

Parameters:
- `id`: Game ID (required)
- `type`: Set to "boardgame" (required)
- `stats`: Include ratings and statistics (1=yes, 0=no)

Response includes:
- Basic game information (name, year, etc.)
- Player counts and play time
- Categories and mechanics
- Description
- Statistics (if requested):
  - Average rating
  - Number of ratings
  - Weight/complexity
  - Number of owners

Example:
```xml
<items termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
    <item type="boardgame" id="13">
        <name type="primary" value="Catan"/>
        <yearpublished value="1995"/>
        <minplayers value="3"/>
        <maxplayers value="4"/>
        <playingtime value="120"/>
        <minplaytime value="60"/>
        <maxplaytime value="120"/>
        <minage value="10"/>
        <description>In Catan...</description>
        <thumbnail>...</thumbnail>
        <image>...</image>
        <statistics>
            <ratings>
                <average value="7.1"/>
                <usersrated value="1000"/>
                <owned value="500"/>
                <averageweight value="2.5"/>
            </ratings>
        </statistics>
    </item>
</items>
```

### Error Handling

HTTP Status Codes:
- 200: Success
- 202: Request accepted, retry
- 429: Too Many Requests
- 500: Server Error
- 504: Gateway Timeout

Error Response Example:
```xml
<error>
    <message>Rate limit exceeded</message>
</error>
```

## Implementation Details

Our API client (`src/api_client/client.py`) implements:
- Rate limiting (2 req/sec)
- Automatic retries with exponential backoff
- Request logging to BigQuery
- Response caching
- Error handling and recovery

Example Usage:
```python
from src.api_client.client import BGGAPIClient

client = BGGAPIClient()
game_data = client.get_thing(13)  # Get Catan data
```

## Terms of Service

When using the BGG XML API2:
1. Include attribution to BoardGameGeek
2. Do not exceed rate limits
3. Cache responses when possible
4. Handle errors gracefully
5. Use data in accordance with BGG's terms

## Resources

- [Official API Documentation](https://boardgamegeek.com/wiki/page/BGG_XML_API2)
- [Terms of Use](https://boardgamegeek.com/xmlapi/termsofuse)
- [BGG Thing IDs](http://bgg.activityclub.org/bggdata/thingids.txt)
