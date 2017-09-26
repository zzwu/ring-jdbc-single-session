(ns ring-jdbc-single-session.core
  (:require [clojure.java.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [ring.middleware.session.store :refer :all])
  (:import java.util.UUID
           org.apache.commons.codec.binary.Base64
           java.sql.SQLException))

(defn serialize-mysql [value]
  (nippy/freeze value))

(defn serialize-postgres [value]
  (nippy/freeze value))

(defn serialize-oracle [value]
  (-> value nippy/freeze Base64/encodeBase64))

(defn serialize-h2 [value]
  (nippy/freeze value))

(defn deserialize-mysql [value]
  (when value
    (nippy/thaw value)))

(defn deserialize-postgres [value]
  (when value
    (nippy/thaw value)))

(defn deserialize-oracle [blob]
  (when blob
    (-> blob (.getBytes 1 (.length blob)) Base64/decodeBase64 nippy/thaw)))

(defn deserialize-h2 [value]
  (when value
    (nippy/thaw value)))

(def serializers
  {:mysql serialize-mysql
   :postgres serialize-postgres
   :oracle serialize-oracle
   :h2 serialize-h2})

(def deserializers
  {:mysql deserialize-mysql
   :postgres deserialize-postgres
   :oracle deserialize-oracle
   :h2 deserialize-h2})

(defn detect-db [db]
  (with-open [conn (jdbc/get-connection db)]
    (let [db-name (.. conn getMetaData getDatabaseProductName toLowerCase)]
      (cond
        (.contains db-name "oracle") :oracle
        (.contains db-name "postgres") :postgres
        (.contains db-name "mysql") :mysql
        (.contains db-name "h2") :h2
        :else (throw (Exception. (str "unrecognized DB: " db-name)))))))

(defn read-session-value [datasource table deserialize key]
  (-> (jdbc/query datasource
                  [(str "select value from " (name table) " where session_id = ?") key])
      first
      :value
      deserialize))

(defn session-by-id [datasource table id]
  (-> (jdbc/query datasource [(str "select * from " (name table) " where id  = ?") id])
      first))

(defn remove-session
  [datasource table key]
  (jdbc/delete! datasource table ["session_id = ?" key]))

(defn update-session-value! [conn table serialize key value]
  (jdbc/update!
   conn
   table
   {:idle_timeout (:ring.middleware.session-timeout/idle-timeout value)
    :absolute_timeout (:ring.middleware.session-timeout/absolute-timeout value)
    :value (serialize value)}
   ["session_id = ? " key])
  key)

(defn insert-session-value! [conn table serialize id value]
  (let [key (str (UUID/randomUUID))]
    (jdbc/insert!
     conn
     table
     {:session_id key
      :idle_timeout (:ring.middleware.session-timeout/idle-timeout value)
      :absolute_timeout (:ring.middleware.session-timeout/absolute-timeout value)
      :id id
      :value (serialize value)})
    key))

(defn fn-with-retry-when-catch-sql-exception
  [f]
  (fn [& params]
    (try
      (apply f params)
      ;;SQLException retry
      (catch SQLException se
        (do
          (log/error :catch-a-SQLException (.getMessage se)
                    :error-code (.getErrorCode se)
                    :eexcption se)
          (try
            ;;retry
            (apply f params)
            (catch SQLException se2
              (do
                (log/error :retry-catch-a-SQLException (.getMessage se2)
                          :error-code (.getErrorCode se2)
                          :eexcption se2)
                (throw se2))))))
      ;;别的exception 直接抛出
      (catch Exception e
        (throw e)))))

(defn do-write-session-inner
  [key value recognize-key datasource table serialize deserialize]
  (let [id (str (get value recognize-key))
        session (session-by-id datasource table id)]
    (if session
      (let [{old-session-id :session_id old-id :id} session]
        (if (= old-session-id key)
          (update-session-value! datasource table serialize key value)
          (do
            (remove-session datasource table old-session-id)
            (insert-session-value! datasource table serialize id value))))
      (if (not= "" id)
        (insert-session-value! datasource table serialize id value)))))

(def do-write-session!
  (-> do-write-session-inner
      ;;retry once
      fn-with-retry-when-catch-sql-exception))

(deftype JdbcStore [recognize-key datasource table serialize deserialize]
  SessionStore
  (read-session
   [_ key]
   (read-session-value datasource table deserialize key))
  (write-session
    [_ key value]
    (do-write-session! key value recognize-key datasource table serialize deserialize))
  (delete-session
   [_ key]
   (jdbc/delete! datasource table ["session_id = ?" key])
   nil))

(ns-unmap *ns* '->JdbcStore)

(defn jdbc-store [key db & [{:keys [table serialize deserialize]
                             :or {table :single_session_store}}]]
  (let [db-type (detect-db db)
        serialize (or serialize (serializers db-type))
        deserialize (or deserialize (deserializers db-type))]
    (JdbcStore. key db table serialize deserialize)))
