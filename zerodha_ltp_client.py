"""
Shared LTP client for every execution script - reads live prices from Redis, kept fresh by
zerodha_ticker_service.py (a single shared process holding the one Zerodha ticker websocket
connection every script reads from, instead of each script running its own - see that file's
docstring for why). Falls back to a direct Zerodha REST /quote/ltp call whenever Redis is
unreachable, or the cached value is missing/older than LTP_STALE_SECONDS - "redis not running" and
"nothing's ticked on this token yet" both degrade gracefully to the same REST path every script
used before this existed, never to a crash or a silently stale price.

No side effects at import time - no connections opened, no broker auth - safe for every execution
script to import unconditionally, unlike execution_rolling_straddle.py (Dhan auth at import) or
this repo's other Zerodha/AliceBlue auth blocks (deliberately NOT duplicated here: each caller
keeps its own already-authenticated REST fetch function and passes it in - see get_ltp() below).
The Redis connection itself is opened lazily on first use and reused after that; if the `redis`
package isn't installed at all, every call here just falls straight through to REST, silently.
"""

import json
import os
import time as time_module

try:
    import redis
except ImportError:  # redis package not installed - every call below falls through to REST
    redis = None

REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
REDIS_CONNECT_TIMEOUT = 0.2  # fail fast to REST rather than let a hung Redis stall the hot path
REDIS_LTP_KEY_PREFIX = 'zerodha:ltp:'
REDIS_SUBSCRIPTIONS_KEY = 'zerodha:subscriptions'
LTP_STALE_SECONDS = 10  # a cached price older than this is treated as if nothing were cached

_redis_client = None
_redis_client_attempted = False
_redis_unavailable_logged = False


def _get_redis_client():
    """Lazily creates and caches the Redis client. A connection failure here doesn't raise - it's
    detected on the first actual command instead (redis-py connects lazily too), which is where
    get_ltp()/register_subscription() already handle the fallback."""
    global _redis_client, _redis_client_attempted
    if redis is None:
        return None
    if not _redis_client_attempted:
        _redis_client_attempted = True
        _redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            socket_connect_timeout=REDIS_CONNECT_TIMEOUT, socket_timeout=REDIS_CONNECT_TIMEOUT,
        )
    return _redis_client


def get_ltp(instrument_token, rest_fetch, log=None, stale_seconds=None):
    """Live LTP for `instrument_token`: Redis first if a fresh enough price is cached there, a
    direct REST fetch otherwise (missing key, stale key, or Redis unreachable - all three fall
    through the same way). `rest_fetch` is a zero-arg callable performing the caller's own
    already-authenticated Zerodha REST LTP fetch - this module never authenticates anything
    itself, every script keeps its own token/session handling exactly as before. `log` (optional)
    gets a one-time warning the first time Redis turns out to be unavailable, so a whole day of
    per-price fallback doesn't spam the log or Telegram. `stale_seconds` (optional) overrides the
    module default LTP_STALE_SECONDS for callers that want a tighter/looser freshness bar than
    every other script sharing this cache."""
    global _redis_unavailable_logged
    max_age = stale_seconds if stale_seconds is not None else LTP_STALE_SECONDS
    client = _get_redis_client()
    if client is not None:
        try:
            raw = client.get(f'{REDIS_LTP_KEY_PREFIX}{instrument_token}')
            if raw is not None:
                data = json.loads(raw)
                if time_module.time() - data['ts'] <= max_age:
                    return float(data['price'])
        except Exception as exc:
            if log is not None and not _redis_unavailable_logged:
                log.warning(
                    f'Redis LTP lookup failed ({exc}) - falling back to REST for the rest of today',
                    extra={'no_telegram': True},
                )
            _redis_unavailable_logged = True
    return rest_fetch()


def get_ltps(token_to_rest_key, rest_fetch_batch, log=None, stale_seconds=None):
    """Batched version of get_ltp() for a caller (like exec_rsv_cont.py's _fetch_market) that
    already fetches several instruments in one Zerodha REST call: `token_to_rest_key` is
    {instrument_token: rest_key}. Every token is tried against Redis first; whatever's missing or
    stale is fetched in ONE batched call via `rest_fetch_batch(rest_keys_list) ->
    {rest_key: price}` - preserving the caller's existing batched-REST-fallback efficiency rather
    than degrading to one REST call per missing token. Returns {instrument_token: price} - a token
    absent from the result means neither Redis nor the batched REST fetch had it. `stale_seconds`
    (optional) overrides the module default LTP_STALE_SECONDS - see get_ltp()."""
    global _redis_unavailable_logged
    max_age = stale_seconds if stale_seconds is not None else LTP_STALE_SECONDS
    result = {}
    missing_tokens = []
    client = _get_redis_client()
    now = time_module.time()
    for token, rest_key in token_to_rest_key.items():
        if client is not None:
            try:
                raw = client.get(f'{REDIS_LTP_KEY_PREFIX}{token}')
                if raw is not None:
                    data = json.loads(raw)
                    if now - data['ts'] <= max_age:
                        result[token] = float(data['price'])
                        continue
            except Exception as exc:
                if log is not None and not _redis_unavailable_logged:
                    log.warning(
                        f'Redis LTP lookup failed ({exc}) - falling back to REST for the rest of today',
                        extra={'no_telegram': True},
                    )
                _redis_unavailable_logged = True
        missing_tokens.append(token)

    if missing_tokens:
        rest_keys = [token_to_rest_key[t] for t in missing_tokens]
        fetched = rest_fetch_batch(rest_keys)
        for token in missing_tokens:
            key = token_to_rest_key[token]
            if key in fetched:
                result[token] = fetched[key]
    return result


def register_subscription(instrument_token):
    """Tells the shared ticker service to start streaming `instrument_token` - adds it to the
    Redis set the service polls for new subscriptions (see zerodha_ticker_service.py). Silently a
    no-op if Redis is unavailable - the caller just keeps getting REST-fallback prices from
    get_ltp() above until this succeeds on some later call."""
    register_subscriptions([instrument_token])


def register_subscriptions(instrument_tokens):
    """Batched register_subscription - one SADD call for several tokens at once."""
    if not instrument_tokens:
        return
    client = _get_redis_client()
    if client is None:
        return
    try:
        client.sadd(REDIS_SUBSCRIPTIONS_KEY, *instrument_tokens)
    except Exception:
        pass
