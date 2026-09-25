# test_flow.py — python test_flow.py
import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2 as pb
import taskflow_pb2_grpc

PORT = 50061  # port dédié aux tests : ne gêne pas un serveur de démo déjà lancé


def expect_error(fn, code):
    """Appelle fn() et vérifie qu'elle échoue avec le code gRPC attendu."""
    try:
        fn()
    except grpc.RpcError as e:
        assert e.code() == code, f"attendu {code}, reçu {e.code()}"
        return
    raise AssertionError(f"attendu une erreur {code}, aucun échec")


def run_tests(stub, log_path):
    # --- TODO(19) : scénario nominal ---
    # 1. Créer "Rapport" assignée à alice (created_by="alice") -> récupérer l'id
    created_task = stub.CreateTask(
        pb.CreateTaskRequest(title="Rapport", created_by="alice")
    )
    task_id = created_task.id

    # 2. GetTask(id) -> titre == "Rapport", statut == TODO
    task = stub.GetTask(pb.GetTaskRequest(id=task_id))
    assert task.title == "Rapport"
    assert task.status == pb.TASK_STATUS_TODO

    # 3. UpdateStatus(id, DONE) -> statut == DONE
    updated_task = stub.UpdateStatus(
        pb.UpdateStatusRequest(id=task_id, status=pb.TASK_STATUS_DONE)
    )
    assert updated_task.status == pb.TASK_STATUS_DONE

    # 4. ListTasks() -> contient au moins 1 tâche ;
    #    ListTasks(status_filter=DONE) -> exactement cette tâche
    all_tasks = list(stub.ListTasks(pb.ListTasksRequest()))
    assert len(all_tasks) >= 1

    done_tasks = list(
        stub.ListTasks(pb.ListTasksRequest(status_filter=pb.TASK_STATUS_DONE))
    )
    assert len(done_tasks) == 1
    assert done_tasks[0].id == task_id

    # --- TODO(20) : erreurs attendues ---
    # Tâche inexistante -> NOT_FOUND
    expect_error(
        lambda: stub.GetTask(pb.GetTaskRequest(id="inconnu"), timeout=3),
        grpc.StatusCode.NOT_FOUND,
    )

    # Titre vide -> INVALID_ARGUMENT
    expect_error(
        lambda: stub.CreateTask(
            pb.CreateTaskRequest(title="", created_by="alice")
        ),
        grpc.StatusCode.INVALID_ARGUMENT,
    )

    # UpdateStatus au même statut -> INVALID_ARGUMENT
    expect_error(
        lambda: stub.UpdateStatus(
            pb.UpdateStatusRequest(id=task_id, status=pb.TASK_STATUS_DONE)
        ),
        grpc.StatusCode.INVALID_ARGUMENT,
    )

    # DONE -> IN_PROGRESS -> INVALID_ARGUMENT (transition invalide)
    expect_error(
        lambda: stub.UpdateStatus(
            pb.UpdateStatusRequest(
                id=task_id, status=pb.TASK_STATUS_IN_PROGRESS
            )
        ),
        grpc.StatusCode.INVALID_ARGUMENT,
    )

    # DeleteTask par "bob" -> PERMISSION_DENIED
    expect_error(
        lambda: stub.DeleteTask(
            pb.DeleteTaskRequest(id=task_id, user="bob")
        ),
        grpc.StatusCode.PERMISSION_DENIED,
    )

    # DeleteTask par "alice" puis GetTask -> NOT_FOUND
    stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, user="alice"))
    expect_error(
        lambda: stub.GetTask(pb.GetTaskRequest(id=task_id)),
        grpc.StatusCode.NOT_FOUND,
    )

    # --- TODO(21) : client streaming ---
    # Créer 3 tâches de titres "alpha", "beta", "alpha beta".
    t1 = stub.CreateTask(pb.CreateTaskRequest(title="alpha", created_by="test"))
    t2 = stub.CreateTask(pb.CreateTaskRequest(title="beta", created_by="test"))
    t3 = stub.CreateTask(
        pb.CreateTaskRequest(title="alpha beta", created_by="test")
    )

    # Envoyer ["alpha", "BETA"] en client streaming
    def keyword_generator():
        yield pb.SearchTasksRequest(keyword="alpha")
        yield pb.SearchTasksRequest(keyword="BETA")

    stats = stub.SearchTasks(keyword_generator())
    assert stats.total_requests == 2
    assert stats.matches["alpha"] == 2
    assert stats.matches["BETA"] == 2

    # Un keyword vide dans le flux -> INVALID_ARGUMENT
    def invalid_keyword_generator():
        yield pb.SearchTasksRequest(keyword="")

    expect_error(
        lambda: stub.SearchTasks(invalid_keyword_generator()),
        grpc.StatusCode.INVALID_ARGUMENT,
    )

    # --- TODO(24) : Subscribe ---
    sub_channel = grpc.insecure_channel(f"localhost:{PORT}")
    sub_stub = taskflow_pb2_grpc.TaskFlowStub(sub_channel)

    events_queue = queue.Queue()

    def listen_events():
        try:
            responses = sub_stub.Subscribe(
                pb.SubscribeRequest(event_types=["DELETED"])
            )
            for event in responses:
                events_queue.put(event)
        except grpc.RpcError:
            pass  # Fermeture propre lors de la fermeture du channel

    thread = threading.Thread(target=listen_events, daemon=True)
    thread.start()

    time.sleep(0.3)  # Attendre l'établissement de l'abonnement

    # Créer et supprimer une tâche pour émettre CREATED (qui doit être filtré) puis DELETED
    temp_task = stub.CreateTask(
        pb.CreateTaskRequest(title="temp_sub_task", created_by="alice")
    )
    stub.DeleteTask(pb.DeleteTaskRequest(id=temp_task.id, user="alice"))

    # Récupérer l'événement : doit être DELETED
    received_event = events_queue.get(timeout=2)
    assert received_event.event_type == "DELETED"
    assert received_event.task.id == temp_task.id

    sub_channel.close()

    # --- TODO(25) : Étape 5 — metadata x-user ---
    with open(log_path) as f:
        log_content = f.read()
    assert "user=testeur" in log_content, "metadata x-user absente des logs serveur"
    assert "code=NOT_FOUND" in log_content, "aucune ligne code=NOT_FOUND dans les logs"

def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    proc = subprocess.Popen(
        [sys.executable, "server.py", "--port", str(PORT)],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    channel = grpc.insecure_channel(f"localhost:{PORT}")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)  # attend le serveur
        # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor("testeur"))
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("✅ Tous les tests passent.")
    finally:
        channel.close()
        proc.terminate()  # toujours exécuté, même si un assert échoue
        proc.wait()


if __name__ == "__main__":
    main()