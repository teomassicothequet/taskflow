# server.py
import argparse
import queue
import threading
import time
import uuid
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import taskflow_pb2
import taskflow_pb2_grpc

_STOP = object()  # sentinelle : sert à débloquer q.get() (voir TODO 8)


def _now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def _status_name(value: int) -> str:
    return taskflow_pb2.TaskStatus.Name(value)   # 2 -> "DONE"


class TaskFlowService(taskflow_pb2_grpc.TaskFlowServicer):
    def __init__(self, slow: bool = False):
        # Modèle interne : dict id -> dict Python. On construit un message
        # protobuf NEUF à chaque réponse (voir self._to_pb) : on ne renvoie
        # jamais un objet partagé qu'un autre thread pourrait modifier.
        self._tasks = {}
        self._lock = threading.RLock()
        # une queue d'événements par abonné
        self._subscribers = []          # liste de (username, event_types, Queue)
        self._subs_lock = threading.Lock()
        self._slow = slow               # bonus B1

    # ---------- TODO(1) ----------
    # CreateTask :
    #   - title vide -> abort(INVALID_ARGUMENT, "title is required")
    #   - created_by vide -> abort(INVALID_ARGUMENT, "created_by is required")
    #   - génère id = str(uuid.uuid4()), statut initial TODO,
    #     created_at = _now(), comments = []
    #   - stocke sous le verrou, publie "CREATED"
    #   - renvoie CreateTaskResponse(task=self._to_pb(t))
    def CreateTask(self, request, context):
        title = request.title.strip()
        created_by = request.created_by.strip()
        if not title:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title is required")
        if not created_by:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "created_by is required")
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "title": title,
            "description": request.description,
            "status": taskflow_pb2.TaskStatus.TODO,
            "assigned_to": request.assigned_to,
            "created_by": created_by,
            "created_at": _now(),
            "comments": []
        }
        with self._lock:
            self._tasks[task_id] = task
            self._publish("CREATED", task_id, created_by, f"{created_by} a créé '{title}'")
        return taskflow_pb2.CreateTaskResponse(task=self._to_pb(task))
    

    # ---------- TODO(2) ----------
    # GetTask : id inconnu -> NOT_FOUND "task <id> not found"
    # (utilisez self._get_or_abort)
    def GetTask(self, request, context):
        task = self._get_or_abort(request.id, context)
        return taskflow_pb2.GetTaskResponse(task=self._to_pb(task))

    # ---------- TODO(3) ----------
    # Server streaming : yield les tâches une par une.
    # Filtres optionnels : HasField("status_filter") et
    # HasField("assigned_filter"). Les deux filtres se combinent en ET.
    # ⚠️ Copiez la liste SOUS le verrou, puis yield HORS du verrou.
    def ListTasks(self, request, context):
        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if request.HasField("status_filter") and task["status"] != request.status_filter:
                continue
            if request.HasField("assigned_filter") and task["assigned_to"] != request.assigned_filter:
                continue
            yield taskflow_pb2.ListTasksResponse(task=self._to_pb(task))        

    # ---------- TODO(4) ----------
    # UpdateStatus (vérifications dans CET ordre) :
    #   - tâche inconnue -> NOT_FOUND
    #   - même statut -> INVALID_ARGUMENT "task already in status X"
    #   - une tâche DONE ne peut plus changer de statut (ni TODO, ni
    #     IN_PROGRESS) -> INVALID_ARGUMENT "cannot reopen a DONE task"
    #   - sinon : mise à jour + événement "STATUS_CHANGED"
    #     author = requested_by, message "alice a passé 'Rapport' à DONE"
    def UpdateStatus(self, request, context):
        task = self._get_or_abort(request.id, context)
        new_status = request.new_status
        if task["status"] == new_status:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"task already in status {_status_name(new_status)}")
        if task["status"] == taskflow_pb2.TaskStatus.DONE:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "cannot reopen a DONE task")
        with self._lock:
            old_status_name = _status_name(task["status"])
            task["status"] = new_status
            self._publish("STATUS_CHANGED", task["id"], request.requested_by,
                          f"{request.requested_by} a passé '{task['title']}' à {_status_name(new_status)}")
        return taskflow_pb2.UpdateStatusResponse(task=self._to_pb(task))        

    # ---------- TODO(5) ----------
    # AssignTask : new_assignee vide -> INVALID_ARGUMENT ;
    # id inconnu -> NOT_FOUND ;
    # réassignation à la même personne -> INVALID_ARGUMENT
    # "task already assigned to X". Événement "ASSIGNED" (author = requested_by).
    def AssignTask(self, request, context):
        new_assignee = request.new_assignee.strip()
        if not new_assignee:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "new_assignee is required")
        task = self._get_or_abort(request.id, context)
        if task["assigned_to"] == new_assignee:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"task already assigned to {new_assignee}")
        with self._lock:
            task["assigned_to"] = new_assignee
            self._publish("ASSIGNED", task["id"], request.requested_by,
                          f"{request.requested_by} a assigné '{task['title']}' à {new_assignee}")
        return taskflow_pb2.AssignTaskResponse(task=self._to_pb(task))

    # ---------- TODO(6) ----------
    # AddComment : texte vide -> INVALID_ARGUMENT ;
    # id inconnu -> NOT_FOUND. Ajoutez un dict {author, text, created_at}
    # à la liste "comments" de la tâche stockée.
    # Événement "COMMENTED". Renvoie la tâche mise à jour.
    def AddComment(self, request, context):
        text = request.text.strip()
        if not text:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "text is required")
        task = self._get_or_abort(request.id, context)
        comment = {
            "author": request.author,
            "text": text,
            "created_at": _now()
        }
        with self._lock:
            task["comments"].append(comment)
            self._publish("COMMENTED", task["id"], request.author,
                          f"{request.author} a commenté '{task['title']}': {text}")
        return taskflow_pb2.AddCommentResponse(task=self._to_pb(task))

    # ---------- TODO(7) ----------
    # DeleteTask : id inconnu -> NOT_FOUND ;
    # requested_by différent du créateur -> PERMISSION_DENIED
    # "only <created_by> can delete this task" ; événement "DELETED".
    def DeleteTask(self, request, context):
        task = self._get_or_abort(request.id, context)
        if task["created_by"] != request.requested_by:
            context.abort(grpc.StatusCode.PERMISSION_DENIED,
                          f"only {task['created_by']} can delete this task")
        with self._lock:
            del self._tasks[request.id]
            self._publish("DELETED", request.id, request.requested_by,
                          f"{request.requested_by} a supprimé '{task['title']}'")
        return taskflow_pb2.DeleteTaskResponse()

    # ---------- TODO(8) ----------
    # Subscribe (server streaming infini) — découpé en DEUX méthodes :
    #
    # Subscribe (fonction normale, PAS un générateur) :
    #   1. q = queue.Queue() ; entry = (username, set(event_types), q)
    #   2. ajouter entry à self._subscribers (sous self._subs_lock)
    #   3. context.add_callback(lambda: q.put(_STOP))
    #      -> gRPC appelle ce callback quand le RPC se termine
    #         (Ctrl+C du client, channel fermé…) : cela débloque q.get()
    #   4. return self._event_stream(entry)
    def Subscribe(self, request, context):
        q = queue.Queue()
        entry = (request.username, set(request.event_types), q)
        with self._subs_lock:
            self._subscribers.append(entry)
        context.add_callback(lambda: q.put(_STOP))
        return self._event_stream(entry)

    # _event_stream (générateur) :
    #   try:    boucle : event = q.get() ; si _STOP -> return ;
    #           si event_types non vide et event.event_type absent -> ignorer ;
    #           sinon yield event
    #   finally: retirer entry de self._subscribers (sous self._subs_lock)
    def _event_stream(self, entry):
        username, event_types, q = entry
        try:
            while True:
                event = q.get()
                if event is _STOP:
                    return
                if event_types and event.event_type not in event_types:
                    continue
                yield event
        finally:
            with self._subs_lock:
                self._subscribers.remove(entry)

    # ---------- TODO(9) ----------
    # SearchKeywords (client streaming) :
    #   - itérer sur les requêtes entrantes (for entry in request_iterator)
    #   - keyword vide -> abort(INVALID_ARGUMENT, "empty keyword")
    #     (attention : abort dans un client-streaming annule tout le flux)
    #   - pour chaque keyword : compter les tâches dont le titre OU la
    #     description contient le keyword (insensible à la casse)
    #   - renvoyer SearchSummary(total_requests=..., results=[
    #         SearchSummary.KeywordHit(keyword=..., match_count=...), ...])
    def SearchKeywords(self, request_iterator, context):
        total_requests = 0
        results = []
        with self._lock:
            tasks = list(self._tasks.values())
        for entry in request_iterator:
            keyword = entry.keyword.strip()
            if not keyword:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty keyword")
            total_requests += 1
            match_count = sum(1 for task in tasks if keyword.lower() in task["title"].lower() or
                              keyword.lower() in task["description"].lower())
            results.append(taskflow_pb2.SearchSummary.KeywordHit(keyword=keyword, match_count=match_count))
        return taskflow_pb2.SearchSummary(total_requests=total_requests, results=results)

    # ----- Utilitaires fournis -----
    def _get_or_abort(self, task_id: str, context) -> dict:
        """Renvoie la tâche stockée ou abort NOT_FOUND (à appeler sous le verrou)."""
        t = self._tasks.get(task_id)
        if t is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {task_id} not found")
        return t

    def _to_pb(self, t: dict) -> taskflow_pb2.Task:
        comments = [taskflow_pb2.Comment(author=c["author"], text=c["text"],
                                         created_at=c["created_at"])
                    for c in t.get("comments", [])]
        return taskflow_pb2.Task(
            id=t["id"], title=t["title"], description=t["description"],
            status=t["status"], assigned_to=t["assigned_to"],
            created_by=t["created_by"], created_at=t["created_at"],
            comments=comments)

    def _publish(self, event_type: str, task_id: str, author: str, message: str):
        """Envoie un événement à tous les abonnés (fourni)."""
        event = taskflow_pb2.TaskEvent(event_type=event_type, task_id=task_id,
                                       author=author, message=message)
        with self._subs_lock:
            for _, _, q in self._subscribers:
                q.put(event)


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--slow", action="store_true", help="bonus B1")
    args = parser.parse_args()

    # Chaque RPC en cours occupe un thread du pool ; un abonné Subscribe
    # en occupe un EN PERMANENCE -> prévoir large.
    # Étape 5 : ajouter interceptors=[LoggingInterceptor()]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    taskflow_pb2_grpc.add_TaskFlowServicer_to_server(TaskFlowService(args.slow), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"TaskFlow server listening on :{args.port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    serve()
