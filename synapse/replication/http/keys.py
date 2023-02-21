# Copyright 2023 The Matrix.org Foundation C.I.C.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
from typing import TYPE_CHECKING, Dict, Tuple

import attrs

from twisted.web.server import Request

from synapse.http.server import HttpServer
from synapse.replication.http._base import ReplicationEndpoint
from synapse.types import JsonDict
from synapse.util.async_helpers import yieldable_gather_results

if TYPE_CHECKING:
    from synapse.crypto.keyring import Keyring, _FetchKeyRequest
    from synapse.server import HomeServer

logger = logging.getLogger(__file__)


class ReplicationFetchKeysEndpoint(ReplicationEndpoint):
    """Another worker is asking us to fetch keys for a homeserver X.

    The request looks like:

        POST /_synapse/replication/fetch_keys
        {
            keys_to_fetch: [
                {
                    "server_name": "example.com",
                    "minimum_valid_until_ts": 123456,
                    "key_ids": ["ABC", "DEF"]
                }
            ]
        }

    Returning

        200 OK

        {
            "example.com": {
                "ABC": {
                    "verify_key": ...
                    "valid_until_ts": 01189998819991197253,
                },
                "DEF": {},
            }
        }
    """

    NAME = "fetch_keys"
    PATH_ARGS = ()
    METHOD = "POST"
    WAIT_FOR_STREAMS = False

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)

        self._keyring: "Keyring" = hs.get_keyring()

    async def _handle_request(  # type: ignore[override]
        self,
        request: Request,
        content: JsonDict,
    ) -> Tuple[int, JsonDict]:
        parsed_requests = [
            _FetchKeyRequest(**entry) for entry in content["keys_to_fetch"]
        ]

        results = await yieldable_gather_results(
            self._keyring.fetch_keys,
            parsed_requests,
        )

        merged_results: Dict[str, Dict[str, JsonDict]] = {}
        for result in results:
            for server_name, keys_for_server in result.items():
                merged_results.setdefault(server_name, {})
                for key_id, key_result in keys_for_server.items():
                    merged_results[server_name][key_id] = attrs.asdict(key_result)

        logger.info("DMR: %s", json.dumps(merged_results))
        return (200, merged_results)

    @staticmethod
    async def _serialize_payload(*, keys_to_fetch: "_FetchKeyRequest") -> JsonDict:  # type: ignore[override]
        return {"keys_to_fetch": keys_to_fetch}


def register_servlets(hs: "HomeServer", http_server: HttpServer) -> None:
    ReplicationFetchKeysEndpoint(hs).register(http_server)
