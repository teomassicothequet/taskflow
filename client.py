# client.py
import argparse
import threading

import grpc

import taskflow_pb2
import taskflow_pb2_grpc

STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
received_events = []  # pour l'option 9 (débug)


def print_event(event):
    print(f"\n🔔 [{event.event_type}] {event.author}: {event.message}\n> ",
          end="", flush=True)


# ---------- TODO(10) ----------
# Thread d'écoute : s'abonner via
#   stub.Subscribe(SubscribeRequest(username=..., event_types=[...]))
# puis for event in stream: mémoriser dans received_events + print_event(event)
# Entourez le tout d'un try/except grpc.RpcError : si le serveur tombe,
# afficher UNE ligne propre (code + details), pas une traceback.
# (CANCELLED = c'est nous qui quittons : ne rien afficher.)
def listen_events(stub, username, event_types):
    try:
        request = taskflow_pb2.SubscribeRequest(username=username, event_types=event_types)
        for event in stub.Subscribe(request):
            received_events.append(event)
            print_event(event)
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.CANCELLED:
            print(f"\n❌ Erreur gRPC [{e.code().name}] : {e.details()}\n> ", end="", flush=True)


def print_task(task):
    print(f"  [{STATUS_NAMES[task.status]:12}] {task.id[:8]}… "
          f"« {task.title} » → {task.assigned_to or 'non assignée'} "
          f"({len(task.comments)} commentaire(s))")


def main():
    parser = argparse.ArgumentParser(description="Client TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", required=True)
    parser.add_argument("--events", default="",
                        help="filtre, ex: CREATED,DELETED (vide = tout)")
    parser.add_argument("--no-listen", action="store_true")
    parser.add_argument("--timeout", type=float, default=3, help="bonus B1")
    args = parser.parse_args()

    channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor(args.user))
    stub = taskflow_pb2_grpc.TaskFlowStub(channel)
    T = args.timeout  # à passer en timeout=T sur TOUS les appels (sauf Subscribe)

    event_types = [e.strip().upper() for e in args.events.split(",") if e.strip()]

    if not args.no_listen:
        threading.Thread(target=listen_events,
                         args=(stub, args.user, event_types),
                         daemon=True).start()

    while True:
        print("""
=== TaskFlow ===  (utilisateur: {u})
 1. Créer une tâche        6. Commenter une tâche
 2. Lister les tâches      7. Supprimer une tâche
 3. Voir une tâche         8. Recherche multi-mots-clés (streaming)
 4. Changer le statut      9. Événements reçus (aide au débug)
 5. Réassigner             0. Quitter""".format(u=args.user))

        try:
            choice = input("choix > ").strip()
            if choice == "1":
                # ---------- TODO(11) ----------
                # Demander title/description/assigné, appeler CreateTask
                # (created_by=args.user, timeout=T), afficher l'id retourné.
                title = input("Titre de la tâche : ").strip()
                description = input("Description de la tâche : ").strip()
                assigned_to = input("Assigné à (laisser vide pour non assignée) : ").strip() or None
                request = taskflow_pb2.CreateTaskRequest(
                    title=title,
                    description=description,
                    assigned_to=assigned_to,
                    created_by=args.user
                )
                response = stub.CreateTask(request, timeout=T)
                print(f"Tâche créée avec l'ID : {response.id}")
                
            elif choice == "2":
                # ---------- TODO(12) ----------
                # Proposer un filtre statut (vide = tous) et un filtre
                # assigné (vide = tous), appeler ListTasks en streaming.
                # Filtre vide -> NE PAS affecter le champ optional.
                status_filter = input("Filtrer par statut (TODO, IN_PROGRESS, DONE) ou laisser vide pour tous : ").strip().upper()
                assigned_filter = input("Filtrer par assigné (laisser vide pour tous) : ").strip() or None
                request = taskflow_pb2.ListTasksRequest()
                if status_filter in STATUS_NAMES.values():
                    request.status = taskflow_pb2.TaskStatus.Value(status_filter)
                if assigned_filter:
                    request.assigned_to = assigned_filter
                print("Liste des tâches :")
                for task in stub.ListTasks(request, timeout=T):
                    print_task(task)

            elif choice == "3":
                # ---------- TODO(13) ----------
                # GetTask : afficher la tâche ET ses commentaires
                # (auteur, date ISO via c.created_at.ToDatetime(), texte).
                task_id = input("ID de la tâche à voir : ").strip()
                request = taskflow_pb2.GetTaskRequest(id=task_id)
                task = stub.GetTask(request, timeout=T)
                print_task(task)
                print("Commentaires :")
                for comment in task.comments:
                    created_at_iso = comment.created_at.ToDatetime().isoformat()
                    print(f"  - {comment.author} ({created_at_iso}) : {comment.text}")
                
            elif choice == "4":
                # ---------- TODO(14) ----------
                # Menu TODO/IN_PROGRESS/DONE -> UpdateStatus
                # (requested_by=args.user).
                task_id = input("ID de la tâche à mettre à jour : ").strip()
                print("Choisissez le nouveau statut :")
                for code, name in STATUS_NAMES.items():
                    print(f"  {code}. {name}")
                new_status_code = int(input("Nouveau statut (0, 1 ou 2) : ").strip())
                if new_status_code not in STATUS_NAMES:
                    print("Statut invalide.")
                    continue
                request = taskflow_pb2.UpdateStatusRequest(
                    id=task_id,
                    new_status=new_status_code,
                    requested_by=args.user
                )
                stub.UpdateStatus(request, timeout=T)
                print("Statut mis à jour avec succès.")

            elif choice == "5":
                # ---------- TODO(15) ----------
                # AssignTask (requested_by=args.user).
                task_id = input("ID de la tâche à réassigner : ").strip()
                new_assigned_to = input("Nouvel assigné (laisser vide pour non assignée) : ").strip() or None
                request = taskflow_pb2.AssignTaskRequest(
                    id=task_id,
                    new_assigned_to=new_assigned_to,
                    requested_by=args.user
                )
                stub.AssignTask(request, timeout=T)
                print("Assignation mise à jour avec succès.")

            elif choice == "6":
                # ---------- TODO(16) ----------
                # AddComment (texte multi-mots, author=args.user).
                task_id = input("ID de la tâche à commenter : ").strip()
                print("Entrez le texte du commentaire (finir par une ligne vide) :")
                lines = []
                while True:
                    line = input()
                    if line == "":
                        break
                    lines.append(line)
                comment_text = "\n".join(lines)
                request = taskflow_pb2.AddCommentRequest(
                    id=task_id,
                    text=comment_text,
                    author=args.user
                )
                stub.AddComment(request, timeout=T)
                print("Commentaire ajouté avec succès.")

            elif choice == "7":
                # ---------- TODO(17) ----------
                # DeleteTask (requested_by=args.user).
                task_id = input("ID de la tâche à supprimer : ").strip()
                request = taskflow_pb2.DeleteTaskRequest(
                    id=task_id,
                    requested_by=args.user
                )
                stub.DeleteTask(request, timeout=T)
                print("Tâche supprimée avec succès.")

            elif choice == "8":
                # ---------- TODO(18) ----------
                # Client streaming : demander des mots-clés un par un
                # (ligne vide = fin), construire un GÉNÉRATEUR Python qui
                # yield les SearchEntry, appeler SearchKeywords(generator)
                # et afficher le SearchSummary (total + résultats).
                keywords = []
                print("Entrez les mots-clés (ligne vide pour terminer) :")
                while True:
                    keyword = input("Mot-clé : ").strip()
                    if not keyword:
                        break
                    keywords.append(keyword)

                def search_entries():
                    for keyword in keywords:
                        yield taskflow_pb2.SearchEntry(keyword=keyword)

                summary = stub.SearchKeywords(search_entries(), timeout=T)
                print(f"Total : {summary.total}")
                print("Résultats :")
                for result in summary.results:
                    print_task(result)

            elif choice == "9":
                print(f"{len(received_events)} événement(s) reçu(s)")
            elif choice == "0":
                print("Au revoir !")
                break
        except grpc.RpcError as e:
            print(f"❌ Erreur gRPC [{e.code().name}] : {e.details()}")
        except (KeyboardInterrupt, EOFError):
            print()
            break
    channel.close()


if __name__ == "__main__":
    main()
