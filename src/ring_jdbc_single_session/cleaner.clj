(ns ring-jdbc-single-session.cleaner
  (:require [clojure.java.jdbc :as jdbc]))

(defn remove-sessions [conn table]
  (let [t (System/currentTimeMillis)]
    (jdbc/delete! conn table ["? - idle_timeout < 0 or ? - absolute_timeout < 0" t t])))

(defprotocol Stoppable
  "Something that can be stopped"
  (stopped? [_] "Return true if stopped, false otherwise")
  (stop     [_] "Stop (idempotent)"))

(defn sleep [millis]
  (let [timeout (+ millis (System/currentTimeMillis))]
    (while (< (System/currentTimeMillis) timeout)
      (try (Thread/sleep (- timeout (System/currentTimeMillis)))
        (catch InterruptedException _
          (.interrupt ^Thread (Thread/currentThread)))))))

(defn start-cleaner
  ([db]
    (start-cleaner db {}))
  ([db {:keys [interval-secs table]
        :or {interval-secs 60
             table :single_session_store}}]
    (let [state  (atom :running)
          interval-ms (* 1000 interval-secs)]
      (-> (fn runner []
            (when @state
              (remove-sessions db table)
              (sleep interval-ms)))
          (Thread.)
          (.start))
      (reify Stoppable
        (stopped? [_] (not @state))
        (stop     [_] (swap! state (constantly false)))))))

(defn stop-cleaner [session-cleaner]
  {:pre [(satisfies? Stoppable session-cleaner)]}
  (.stop session-cleaner))
