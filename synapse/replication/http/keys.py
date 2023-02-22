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
import logging
from typing import TYPE_CHECKING, Dict, List, Tuple

import attr

from twisted.web.server import Request

from synapse.crypto.types import _FetchKeyRequest
from synapse.http.server import HttpServer
from synapse.replication.http._base import ReplicationEndpoint
from synapse.storage.keys import FetchKeyResult
from synapse.types import JsonDict
from synapse.util.async_helpers import yieldable_gather_results

if TYPE_CHECKING:
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

    We would normally return a group of FetchKeyResponse structs like the
    normal code path does, but FetchKeyResponse holds a nacl.signing.VerifyKey
    which is not JSON-serialisable. Instead, for each requested key we respond
    with a boolean: `true` meaning we fetched this key, and `false` meaning we
    didn't.

    The response takes the form:

        200 OK
        {
            "fetched_count": 1
        }
    """

    NAME = "fetch_keys"
    PATH_ARGS = ()
    METHOD = "POST"
    WAIT_FOR_STREAMS = False

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)

        self._keyring = hs.get_keyring()

    async def _handle_request(  # type: ignore[override]
        self,
        request: Request,
        content: JsonDict,
    ) -> Tuple[int, JsonDict]:
        parsed_requests = [
            _FetchKeyRequest(**entry) for entry in content["keys_to_fetch"]
        ]

        results: List[
            Dict[str, Dict[str, FetchKeyResult]]
        ] = await yieldable_gather_results(
            self._keyring.fetch_keys,
            parsed_requests,
        )

        # We don't send the results back to the requesting worker directly.
        # Doing so would mean faffing around trying to JSON-serialise a
        # nacl.signing.VerifyKey, which isn't our business to do. Instead, just say
        # how many keys we fetched.
        fetched_count = sum(
            len(keys_for_server)
            for entry in results
            for server_name, keys_for_server in entry.items()
        )

        return 200, {"fetched_count": fetched_count}

    @staticmethod
    async def _serialize_payload(*, keys_to_fetch: List[_FetchKeyRequest]) -> JsonDict:  # type: ignore[override]
        return {"keys_to_fetch": [attr.asdict(key) for key in keys_to_fetch]}


def register_servlets(hs: "HomeServer", http_server: HttpServer) -> None:
    ReplicationFetchKeysEndpoint(hs).register(http_server)
