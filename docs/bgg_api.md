# BoardGameGeek XML API2 Documentation

This document outlines the BoardGameGeek XML API2 endpoints and usage guidelines used in this project.

## Authentication

BoardGameGeek's XML API2 **requires authentication**. Anonymous requests are rejected
with `401 Unauthorized` and the body `Unauthorized. See
https://boardgamegeek.com/using_the_xml_api`. Set `BGG_API_TOKEN`; the client sends it
as a Bearer token. In CI it comes from the `BGG_API_TOKEN` repository secret.

> This changed on BGG's side in September 2026 — the API was previously public.

Note that the token governs the **API** only. BGG's Cloudflare protection is separate
and applies to the HTML/sitemap paths, which can return `403` for an IP even while
authenticated API requests from that same IP succeed.

## API Base URL
```
https://boardgamegeek.com/xmlapi2/
```

## Rate Limiting
- 2 requests per second is the documented ceiling, but **sustained** traffic at that
  rate is throttled: a 0.5s cadence drew `429 Rate limit exceeded` after ~48
  consecutive requests (measured 2026-09-14). The limit appears to be on burst rate
  rather than daily volume — the detail pipeline issues ~50 requests/day spread over
  ~30 minutes and never sees a 429.
- Bulk callers should pass a larger `throttle_delay` to `BGGAPIClient` (the probe uses
  2.0s) rather than relying on the default.
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

## Discovering new items

BGG publishes **no endpoint that enumerates the catalog or reports newly added
items** — `/thing` needs IDs you already have, and `/search` matches on name. IDs
must therefore be discovered some other way. This project uses two:

- **API probe** (`src/modules/id_probe_fetcher.py`, default): walks the numeric ID
  space above the highest known ID. IDs come from one pool shared with `rpgitem`,
  `videogame`, `bgsleeve` and others, so each item's own `type` decides whether it is
  kept. Requires `type` to be omitted from the request, or expansions and accessories
  are filtered out.
- **Sitemap crawl** (`src/modules/id_fetcher_browser.py`): scrapes BGG's sitemaps with
  a stealth browser. Sees the whole catalog rather than only the frontier, so it also
  catches items added *below* the frontier (~2% of arrivals), but depends on getting
  past Cloudflare.

Measured against the live API on 2026-09-14: ~45% of IDs near the frontier are board
game items, the largest gap between consecutive real IDs was 8 (112 across populated
space), and a probe replayed over a known range recovered 100% of its items — plus 16
the sitemap had missed, since the sitemap is regenerated periodically while the API is
live.

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
