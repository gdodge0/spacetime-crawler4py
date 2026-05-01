import requests
import cbor
import time

from utils.response import Response

MAX_SIZE = 10 * 1024 * 1024 # 10 mb

class ResponseTooLarge(Exception):
    pass

def get_with_limit(host, port, url, config):
    response = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")],
        stream=True,
    )
    try:
        content_length = response.headers.get("Content-Length")
        if content_length is not None and int(content_length) > MAX_SIZE:
            raise ResponseTooLarge(f"Content-length exceeds MAX_SIZE [headers]")

        total_b = 0
        chunks = []

        for chunk in response.iter_content(chunk_size=MAX_SIZE):
            # just read as one chunk, we can eat 10mb ram here
            if not chunk:
                continue

            total_b += len(chunk)

            if total_b > MAX_SIZE:
                raise ResponseTooLarge(f"Content-length exceeds MAX_SIZE [observed]")

            chunks.append(chunk)

        # make the response appear to be the same as before (i.e. not streamed, std. request)
        response._content = b"".join(chunks)
        response._content_consumed = True

        return response
    except Exception:
        response.close()
        raise



def download(url, config, logger=None):
    host, port = config.cache_server
    try:
        resp = get_with_limit(host, port, url, config)
    except ResponseTooLarge:
        return Response({
            "error": f"Custom Response error TOO_LARGE with url {url}.",
            "status": 700,
            "url": url})
    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
