# interceptors.py
import collections
import time
from datetime import datetime

import grpc


# ---------- TODO(22) ----------
# LoggingInterceptor (serveur) : à chaque RPC, afficher
#   [HH:MM:SS] METHOD  duration=XXms  code=XX  user=YY
#
# ⚠️ grpc.ServerInterceptor n'a qu'UNE méthode : intercept_service().
#    Elle est appelée AVANT le RPC : continuation(handler_call_details)
#    renvoie un RpcMethodHandler, sans exécuter le RPC. Chronométrer
#    autour de continuation() donnerait donc toujours ~0 ms.
#
# Démarche :
#   1. handler = continuation(handler_call_details) (None -> return None)
#   2. selon le type (handler.unary_unary, .unary_stream, .stream_unary,
#      .stream_stream), envelopper la fonction dans un wrapper qui
#      chronomètre avec time.perf_counter() :
#        - réponse unique  : autour de l'appel à la fonction
#        - réponse en flux : wrapper GÉNÉRATEUR (yield from ...), log à la fin
#   3. reconstruire le handler avec grpc.unary_unary_rpc_method_handler(
#        wrapper, handler.request_deserializer, handler.response_serializer)
#      (idem unary_stream_…, stream_unary_…, stream_stream_…)
#   4. code : context.code() (None = OK ; exception non-abort = UNKNOWN)
#   5. user : dict(handler_call_details.invocation_metadata).get("x-user")
#   Pensez à print(..., flush=True).

class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        method = handler_call_details.method
        user = dict(handler_call_details.invocation_metadata).get("x-user")

        def log(context, started):
            code = context.code() or grpc.StatusCode.UNKNOWN
            print(
                f"[{datetime.now():%H:%M:%S}] {method} "
                f"duration={(time.perf_counter() - started) * 1000:.0f}ms "
                f"code={code.name} user={user}",
                flush=True,
            )

        if handler.unary_unary:
            def unary_unary(request, context):
                started = time.perf_counter()
                try:
                    return handler.unary_unary(request, context)
                finally:
                    log(context, started)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary, handler.request_deserializer, handler.response_serializer)

        if handler.unary_stream:
            def unary_stream(request, context):
                started = time.perf_counter()
                try:
                    yield from handler.unary_stream(request, context)
                finally:
                    log(context, started)

            return grpc.unary_stream_rpc_method_handler(
                unary_stream, handler.request_deserializer, handler.response_serializer)

        if handler.stream_unary:
            def stream_unary(request_iterator, context):
                started = time.perf_counter()
                try:
                    return handler.stream_unary(request_iterator, context)
                finally:
                    log(context, started)

            return grpc.stream_unary_rpc_method_handler(
                stream_unary, handler.request_deserializer, handler.response_serializer)

        if handler.stream_stream:
            def stream_stream(request_iterator, context):
                started = time.perf_counter()
                try:
                    yield from handler.stream_stream(request_iterator, context)
                finally:
                    log(context, started)

            return grpc.stream_stream_rpc_method_handler(
                stream_stream, handler.request_deserializer, handler.response_serializer)

        return handler


# ---------- TODO(23) ----------
# HeaderInterceptor (client) : ajoute le metadata ("x-user", <pseudo>) à
# CHAQUE appel — y compris ListTasks, Subscribe et SearchKeywords, d'où
# l'héritage des 4 interfaces.
# grpc.ClientCallDetails est abstraite : utilisez la classe _ClientCallDetails
# ci-dessous pour construire des détails modifiés, puis
#   return continuation(nouveaux_details, request)
class HeaderInterceptor(grpc.UnaryUnaryClientInterceptor,
                        grpc.UnaryStreamClientInterceptor,
                        grpc.StreamUnaryClientInterceptor,
                        grpc.StreamStreamClientInterceptor):
    def __init__(self, user: str):
        self._user = user

    def _with_user(self, details):
        metadata = list(details.metadata) if details.metadata is not None else []
        metadata.append(("x-user", self._user))
        return _ClientCallDetails(
            details.method, details.timeout, metadata,
            details.credentials, details.wait_for_ready, details.compression,
        )

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._with_user(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._with_user(details), request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        return continuation(self._with_user(details), request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        return continuation(self._with_user(details), request_iterator)
    
class _ClientCallDetails(
        collections.namedtuple(
            "_ClientCallDetails",
            ("method", "timeout", "metadata", "credentials",
             "wait_for_ready", "compression")),
        grpc.ClientCallDetails):
    pass


