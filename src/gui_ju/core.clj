(ns gui-ju.core)

(require '[clojure.edn :as edn]
         '[clojure.string :as string])

(defn is-op
  "check op has a certain :f"
  [op type]
  (= type
     (get op :f)))

(defn is-cas?
  "check op is cas op"
  [op]
  (is-op op :cas))

(defn is-write?
  "check op is write op"
  [op]
  (is-op op :write))

(defn cas-read
  "check cas reads value"
  [cas value]
  (and (is-cas? cas)
       (= value
          (first (get cas :value)))))

(defn cas-read?
  [value]
  (fn [x]
    (cas-read x value)))

(defn cas-write
  [cas value]
  (and (is-cas? cas)
       (= value
          (last (get cas :value)))))

(defn write-value
  [op value]
  (and (is-write? op)
       (= value
          (get op :value))))

(defn write-value?
  [value]
  (fn [x]
    (write-value x value)))

(defn replace-ak
  "replace :[applied] keyword by :applied for further parsing of edn"
  [s]
  (string/replace s ":[applied]" ":applied"))

(defn read-edn-history
  "read history (list of event objects) from edn file"
  [edn-file]
  (->> edn-file
       slurp
       clojure.string/split-lines
       (map replace-ak)
       (map edn/read-string)))

(defn ok-history-fold-op
  "Folds an operation into a ok history, keeping track of outstanding
  invocations.

  History is our ok history of ok events: a transient vector. Index is a
  transient map of processes to the index of their most recent invocation. Note
  that we assume processes are singlethreaded; e.g. they do not perform
  multiple invocations without receiving responses."
  [[history invocations index] op]
  (condp = (:type op)
    ; An invocation; remember where it is
    :invoke
    (do
      ; Enforce the singlethreaded constraint.
      (when-let [prior (get index (:process op))]
        (throw (RuntimeException.
                (str "Process " (:process op) " already running "
                     (pr-str (get history prior))
                     ", yet attempted to invoke "
                     (pr-str op) " concurrently"))))
      [history
       (conj invocations op)
       (assoc index (:process op) (count invocations))])

    ; A ok; construct a new ok event
    :ok
    (let [i           (get index (:process op))
          ;; _           (println i)
          _           (assert i (str "Process completed an operation without a "
                                     "prior invocation: "
                                     (pr-str op)))
          invocation  (nth invocations i)
          ;; _           (println invocation)
          ok-event (-> invocation
                       (assoc :start-time (:time invocation))
                       (assoc :end-time (:time op))
                       (dissoc :time))]
      [(conj history ok-event)
       invocations
       (dissoc index (:process op))])

    ; A failure; fill in either value.
    :fail
    (let [i           (get index (:process op))
          _           (assert i (str "Process failed an operation without a "
                                     "prior invocation: "
                                     (pr-str op)))]
      [history
       invocations
       (dissoc index (:process op))])

    ; No change for info messages
    :info
    [history invocations index]))

(defn extract-ok-history
  "extract ok events from original edn-history
   an ok event is constructed from two original events, for example,
   {:type :invoke, :f :cas, :value [0 7], :time 23853977497, :process 0, :index 56}
   {:type :ok, :f :cas, :value [0 7], :time 23862088836, :process 0, :index 57}
   will be extracted as
   {:type :invoke, :f :cas, :value [0 7], :time 23853977497, :process 0, :index 56, :start-time 23853977497, :end-time 23862088836}"
  [history]
  (->> history
       (reduce ok-history-fold-op [[] [] {}])
       first))

(defn extract-value-valid-cas-recur
  [cas-history value-valid-cas-history]
  (let [current-cas   (last value-valid-cas-history)
        current-value (last (get current-cas :value))
        next-cas-list (filter (cas-read? current-value) cas-history)
        next-cas      (first next-cas-list)]
    (cond
      ;; loop with repeated write
      (> (count value-valid-cas-history) (count cas-history))
      (throw (RuntimeException.
              (str "Invalid Read Mapping for Value-Valid Extract(loop): "
                   [current-cas next-cas])))
      ;; ending case
      (= 0 (count next-cas-list)) value-valid-cas-history
      ;; recursion
      (= 1 (count next-cas-list)) (extract-value-valid-cas-recur
                                   cas-history
                                   (conj value-valid-cas-history next-cas))
      :else (throw (RuntimeException.
                    (str "Invalid Read Mapping for Value-Valid Extract(repeated read): "
                         next-cas-list))))))

(defn extract-value-valid-history
  "find the only valid history for [v, v'] of every cas
   
   ok-history must be the result of extract-ok-history"
  [ok-history]
  (let [write-op-list   (filter is-write? ok-history)
        write-op        (first write-op-list)
        cas-op-list     (filter is-cas? ok-history)
        original-value  (get write-op :value)
        first-cas-list  (filter (cas-read? original-value) cas-op-list)
        first-cas       (first first-cas-list)
        ;; _ (println first-cas)
        value-valid-cas-history (extract-value-valid-cas-recur cas-op-list [first-cas])
        ;; _ (println value-valid-cas-history)
        ]
    (cond
      (not (= 1 (count write-op-list)))  [:not-one-write write-op-list]
      (empty? first-cas-list) [:no-first-cas []]

      (> (count cas-op-list)
         (count value-valid-cas-history))
      [:incomplete-cas-history value-valid-cas-history]

      ;; valid case
      :else [:valid (cons write-op value-valid-cas-history)])))


;;;;;;;;;;;; Verify Read Mapping
(defn get-same-write-op-list
  [op ok-history]
  (let [write-v (case (get op :f)
                  :write (get op :value)
                  :cas (last (get op :value)))]
    (filter #(or (write-value % write-v)
                 (cas-write % write-v)) ok-history)))

(defn get-same-read-op-list
  [op ok-history]
  (case (get op :f)
    :write []
    :cas (let [read-v (first (get op :value))]
           (filter (cas-read? read-v) ok-history))))

(defn valid-RM-history?
  "check read mapping of ok-history is ok, i.e.
   1. unique read
   2. unique write"
  [ok-history]
  (if (empty? ok-history)
    ;; ending case
    [:valid []]
    ;; recursive case
    (let [op                 (first ok-history)
          same-write-op-list (get-same-write-op-list op ok-history)
          same-read-op-list  (get-same-read-op-list op ok-history)]
      (cond
        ;; repeat write for same value
        (< 1 (count same-write-op-list)) [:repeat-write same-write-op-list]
        ;; repeat read for same value
        (< 1 (count same-read-op-list))  [:repeat-read  same-read-op-list]
        ;; recursion
        :else (valid-RM-history? (rest ok-history))))))

(defn time-valid-op-against-list
  [op op-list]
  (if (empty? op-list)
    [:valid []]
    (let [first-op-in-list (first op-list)]
      (if (< (get op :start-time)
             (get first-op-in-list :end-time))
        (time-valid-op-against-list op (rest op-list))
        [:invalid [op first-op-in-list]]))))

(defn time-valid-history?
  [value-valid-history]
  (if (empty? value-valid-history)
    [:valid []]
    (let [first-op (first value-valid-history)
          rest-op-list (rest value-valid-history)
          [op-vs-list-valid evidence-list] (time-valid-op-against-list first-op rest-op-list)]
      (case op-vs-list-valid
        :invalid
        [:invalid evidence-list]

        :valid
        (time-valid-history? rest-op-list)))))

(defn linear-check-edn [edn-file]
  (let [history (read-edn-history edn-file)
        ok-history (extract-ok-history history)
        [valid-RM evidence-list] (valid-RM-history? ok-history)]
    ;; (println "ok-history:")
    ;; (doseq [h ok-history]
      ;; (println h))
    (case valid-RM
      ;; two invalid cases
      :repeat-read  [:invalid-rm-repeated-read evidence-list]
      :repeat-write [:invalid-rm-repeated-write evidence-list]

      ;; valid read mapping, progress to value valid
      :valid
      (let [[valid-value value-valid-history] (extract-value-valid-history ok-history)]
        (case valid-value
          ;; three invalid cases
          :not-one-write [:invalid-rm-not-one-write value-valid-history]
          :no-first-cas [:invalid-value-no-first-cas []]
          :incomplete-cas-history [:invalid-value-incomplete-cas-chain value-valid-history]

          ;; valid value chain; progress to time valid
          :valid
          (let [[valid-time time-valid-history] (time-valid-history? value-valid-history)]
            (case valid-time
              :valid   [:valid []]
              :invalid [:invalid-time-conflict time-valid-history])))))))

(defn -main [& args] ; & creates a list of var-args
  (if (seq args)
    ; Foreach arg, print the arg...
    (doseq [arg args]
      (let [[check-result evidence-list] (linear-check-edn arg)]
        (println {:valid? check-result :evidence evidence-list})))

    ; Handle failure however here
    (throw (Exception. "Must have at least one argument!"))))