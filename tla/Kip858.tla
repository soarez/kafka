-------------------------------- MODULE Kip858 --------------------------------
(**
https://cwiki.apache.org/confluence/display/KAFKA/KIP-858%3A+Handle+JBOD+broker+disk+failure+in+KRaft

 The main issue that KIP-858 aims to address is the handling of
 log directory failures.

 With a single log directory configured per Broker, the Controller can rely
 on the Broker heartbeat to determine whether a leader and ISR update is
 required. But with multiple log directories, we'd like Brokers to continue
 serving replicas off of the available storage devices when one of them
 fails.

 The Controller needs to know which partitions require a leader & ISR update.
 There are two main ways this can be achieved:

   a) The Broker can indicate the list of partitions that resided in the
   log diretory upon failure, in an RPC to the Controller.

   b) The Broker can incrementally inform the Controller about
   replica to log directory placement, aiming to always keep the
   Controller informed about what partitions reside in each log directory.
   In this case, upon a directory failure, the Broker does not need to
   enumerate the resident partitions, but merely needs to identify the
   failed log directory in a request to the Controller.

Approach a) has been deemed unfit, as the number of partitions in each
log directory may be uncomfortably large to be transmitted to the Controller.

The challenge with approach b) is that any failure in the syncing of the
replica-to-logDir assignment with the Controller may result in indefinite
unavailability for any partitions that were hosted in an offline logDir,
but whose logDir assignment was unkown (or incorrect) from the perspective
of the Controller.

This TLA+ specification aims to check for edge cases in this syncing.

Whatever happens, when log directory fails, the Controller needs to react
with an update to the leadership and ISR for every partition hosted in
that log directory.

Simplifications:

 a) Modelling only a single Broker and a single Controller

 b) We skip Broker registration, instead the Controller is already aware
 of the available log direcotories in the Broker

 c) Once a logDir fails, it won't be brought back up, Broker restarts are
 not modelled

 d) Intra-broker x-logDir replica movements are not modelled

 *)

EXTENDS FiniteSets, Naturals, Sequences, TLC

\* The number of distinct log directories in the Broker
CONSTANT NumLogDirs
ASSUME NumLogDirs \in 1..20

\* Restrict the model to a maximum number of partitions
CONSTANT MaxPartitions
ASSUME MaxPartitions \geq 1

\* Utils
None == "None"
EmptyAssignments == [ partition \in {} |-> None]
Set2Seq(set) ==
    \* convert a set into one of its possible sequences
    CHOOSE seq \in UNION {[1..Cardinality(set) -> set]} : {seq[idx]: idx \in DOMAIN seq} = set
AllLogDirs == {"dir" \o (ToString(idx)) : idx \in 1..NumLogDirs}
AllPartitions == {"p" \o (ToString(idx)) : idx \in 1..MaxPartitions}

VARIABLES
    \* Model state - auxiliary variables for model checking
    mInflightAssignments,     \* AssignReplicasToDirs RPC, from the Broker to the Controller, in flight
    mInflightFailures,        \* Notification of logDir failure, from a BrokerHeartbeatRequest RPC call
    mNewRecords,              \* New metadata records for new partitions, not yet fetched by the Broker
    \* Broker state
    bLogDirs,                 \* mapping of log directory to replica set
    bPending,                 \* assignments, pending to be communicated to the Controller
    bMetadata,                \* broker's view of the assignments in the cluster metadata, updated as metadata is consumed
    bOffline,                 \* set of logDirs known to be offline by the Broker
    \* Controller state
    cAssignments,             \* replica to logDir assignment
    cOffline,                 \* set of logDirs known to be offline by the Controller
    cUpdated                  \* set of partitions, for which the Controller has had a chance to issue Leadership & ISR updates due to logDir failures
vars == <<mInflightAssignments, mInflightFailures, mNewRecords, bOffline, bLogDirs, bPending, bMetadata, cAssignments, cOffline, cUpdated>>

TypeOK ==
    /\ mInflightAssignments \subseteq [partition : AllPartitions, logDir: AllLogDirs]
    /\ mInflightFailures \subseteq {"dir" \o (ToString(idx)) : idx \in 1..NumLogDirs}
    /\ \A record \in {mNewRecords[i] : i \in DOMAIN mNewRecords} :
        record \in [partition : AllPartitions, logDir: AllLogDirs \union {None}]
    /\
        /\ DOMAIN bLogDirs \subseteq AllLogDirs
        /\ \A logDir \in DOMAIN bLogDirs : bLogDirs[logDir] \subseteq AllPartitions
    /\ bPending \subseteq [partition : AllPartitions, logDir: AllLogDirs]
    /\
        /\ DOMAIN bMetadata \subseteq AllPartitions
        /\ \A partition \in DOMAIN bMetadata : bMetadata[partition] \in AllLogDirs \union {None}
    /\ bOffline \subseteq AllLogDirs
    /\
        /\ DOMAIN cAssignments \subseteq AllPartitions
        /\ \A partition \in DOMAIN cAssignments : cAssignments[partition] \in AllLogDirs \union {None}
    /\ cOffline \subseteq AllLogDirs
    /\ cUpdated \subseteq AllPartitions

Init ==
    /\ mInflightAssignments = {}
    /\ mInflightFailures = {}
    /\ mNewRecords = <<>>
    /\ bLogDirs = [dir \in { "dir" \o (ToString(idx)) : idx \in 1..NumLogDirs } |-> {}]
    /\ bPending = {}
    /\ bMetadata = EmptyAssignments
    /\ bOffline = {}
    /\ cAssignments = EmptyAssignments
    /\ cOffline = {}
    /\ cUpdated = {}

\* Controller behavior
CHandleAssignReplicasToDirsRpc ==
    /\ mInflightAssignments # {} \* precondition: there are assignments in flight
    \* update the Controller's view of replica to logDir assignment based on the updates from the Broker
    /\ cAssignments' = (
        [ p \in { a.partition : a \in mInflightAssignments} |-> LET x == CHOOSE e \in mInflightAssignments : e.partition = p IN x.logDir ]
        ) @@ cAssignments
    \* publish the updated partition metadata records with the new assigned logDirs
    /\ mNewRecords' = mNewRecords \o Set2Seq({ [partition |-> a.partition, logDir |-> a.logDir] : a \in mInflightAssignments })
    /\ cUpdated' = cUpdated \union { a.partition : a \in { a \in mInflightAssignments : a.logDir \in cOffline} }
    /\ mInflightAssignments' = {} \* clear the inflight request
    /\ UNCHANGED <<mInflightFailures, bOffline, bLogDirs, bPending, bMetadata, cOffline>>
CHandleLogDirFailureNotification ==
    /\ mInflightFailures # {} \* precondition: there is a log dir failure notification in flight
    /\ cUpdated' = cUpdated \union { p \in DOMAIN cAssignments : cAssignments[p] \in mInflightFailures }
    /\ cOffline' = cOffline \union mInflightFailures
    /\ mInflightFailures' = {} \* clear the inflight request
    /\ UNCHANGED <<mInflightAssignments, mNewRecords, bOffline, bLogDirs, bPending, bMetadata, cAssignments>>
CHandleUserCreatePartitionRpc ==
    /\ Cardinality(DOMAIN cAssignments) < MaxPartitions
    /\ LET partition == "p" \o (ToString(Cardinality(DOMAIN cAssignments) + 1))
       IN   /\ cAssignments' = [ p \in {partition} |-> None ] @@ cAssignments
            /\ mNewRecords' = Append(mNewRecords, [partition |-> partition, logDir |-> None])
    /\ UNCHANGED <<mInflightAssignments, mInflightFailures, bOffline, bLogDirs, bPending, bMetadata, cOffline, cUpdated>>
CNext ==
    \/ CHandleUserCreatePartitionRpc
    \/ CHandleAssignReplicasToDirsRpc
    \/ CHandleLogDirFailureNotification

\* Broker behavior
BFetchMetadata ==
    /\ Len(mNewRecords) > 0                         \* precondition: there are new partition records
    /\ LET assignment == Head(mNewRecords) IN       \* process one at a time
        /\ IF assignment.logDir = None /\ Cardinality(bOffline) < NumLogDirs THEN
              LET
                replica == Head(mNewRecords).partition   \* process one at a time
                onlineLogDirs == (DOMAIN bLogDirs) \ bOffline
                logDir == CHOOSE logDir \in onlineLogDirs :      \* select the least loaded logDir
                    \A otherlogDir \in onlineLogDirs :
                        Cardinality(bLogDirs[logDir]) \leq Cardinality(bLogDirs[otherlogDir])
              IN /\ bLogDirs' = [bLogDirs EXCEPT ![logDir] = @ \union {replica}  ]           \* create the replica in the selected logDir
                 /\ bPending' = bPending \union {[partition |-> replica, logDir |-> logDir]} \* register the assingment, pending to be sent to the Controller
           ELSE /\ bLogDirs' = bLogDirs
                /\ bPending' = bPending
        /\ bMetadata' = [ a \in {assignment.partition} |-> assignment.logDir ] @@ bMetadata
    /\ mNewRecords' = Tail(mNewRecords)
    /\ UNCHANGED <<mInflightAssignments, mInflightFailures, bOffline, cAssignments, cOffline, cUpdated>>
BCallAssignReplicasToDirsRpc ==
    /\ bPending # {} \* precondition: there are pending assignments to send
    /\ mInflightAssignments = {} \* precondition: no request in flight
    /\ mInflightAssignments' = bPending
    /\ bPending' = {}
    /\ UNCHANGED <<mInflightFailures, mNewRecords, bLogDirs, bOffline, bMetadata, cAssignments, cOffline, cUpdated>>
BFailLogDir ==
    /\ Cardinality(bOffline) < NumLogDirs \* precondition: not all logDirs are offline
    /\ LET
            logDir == CHOOSE logDir \in DOMAIN bLogDirs: logDir \notin  bOffline
       IN
        /\ bOffline' = bOffline \union {logDir}
        /\ mInflightFailures' = mInflightFailures \union {logDir}
    /\ UNCHANGED <<mInflightAssignments, mNewRecords, bLogDirs, bMetadata, cAssignments, cOffline, cUpdated, bPending>>
BNext ==
    \/ BFetchMetadata
    \/ BCallAssignReplicasToDirsRpc
    \/ BFailLogDir

\* State discovery
Next ==
    \/ CNext
    \/ BNext
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Next) \* Stuttering is not useful

\* Invariants
UpdatesOnlyOfflinePartitions ==
    \* cUpdated represents the set of partitions which the controller has deemed in
    \* need of a leadership and ISR update *due to a logdir failure*, so these
    \* should only include partitions which exist in offline logdirs
    \A partition \in cUpdated : \E logDir \in bOffline : partition \in bLogDirs[logDir]

\* Temporal properties
AlwaysEventuallyControllerKnowsAssignments ==
    \* Controller's information on assignments is exactly matches actual assignments in the Broker
    []<> LET
    controllerPairs == { [replica |-> replica, logDir |-> cAssignments[replica]] : replica \in DOMAIN cAssignments }
    brokerPairs == UNION { { [replica|-> replica, logDir|->logDir] : replica \in bLogDirs[logDir] } : logDir \in DOMAIN bLogDirs }
    IN { c \in controllerPairs : c.logDir # None } = brokerPairs
AlwaysEventuallyBrokerCatchesUpWithMetadataAssignments ==
    \* The Broker's view of the assignments is always eventually updated
    []<> (cAssignments = bMetadata)
AlwaysEventuallyAllLogDirsGoOffline == []<> (Cardinality(bOffline) = NumLogDirs)
AlwaysEventuallyControllerKnowsAllOfflineLogDirs == []<> (Cardinality(cOffline) = Cardinality(bOffline))
AlwaysEventuallyAnyOfflineReplicaGetsLeadershipUpdate ==
    []<> LET offlineReplicas == UNION {{replica : replica \in bLogDirs[logDir]} : logDir \in bOffline}
    IN offlineReplicas = cUpdated

=============================================================================

