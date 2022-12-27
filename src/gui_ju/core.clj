(ns gui-ju.core 
  (:gen-class))

(require '[clojure.edn :as edn]
         '[clojure.string :as string])

(defmacro msectime
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (prn (quot (- (. System (nanoTime)) start#) 1000))
     ret#))

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
  [read-map value-valid-cas-history current-value]
  (let [next-cas      (get read-map current-value)]
    (cond
      (nil? next-cas) value-valid-cas-history
      :else (recur
             read-map
             (conj value-valid-cas-history next-cas)
             (last (get next-cas :value)))
      )))

(defn op-list-into-read-map
  [op-list]
  (->> (map #(let [rv (first (get % :value))] [rv %]) op-list)
       (into {})))

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
        read-map (op-list-into-read-map cas-op-list)
        value-valid-cas-history (extract-value-valid-cas-recur read-map [first-cas] (last (get first-cas :value)))
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


(defn time-valid-history-ax
  [max-start-time value-valid-history]
  (if (empty? value-valid-history)
    [:valid []]
    (let [cur-start-time (get (first value-valid-history) :start-time)
          cur-end-time (get (first value-valid-history) :end-time)]
      (if (< max-start-time cur-end-time)
        (recur (max cur-start-time max-start-time) (rest value-valid-history))
        [:invalid-time-conflict [value-valid-history]]))))

(defn time-valid-history?
  [value-valid-history]
  (if (empty? value-valid-history)
    [:valid []]
    (let [first-op (first value-valid-history)
          rest-op-list (rest value-valid-history)
          first-start-time (get first-op :start-time)]
      (time-valid-history-ax first-start-time rest-op-list))))

(defn linear-check-edn [edn-file]
  (let [history (read-edn-history edn-file)
        ok-history (extract-ok-history history)]
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
              :invalid [:invalid-time-conflict time-valid-history]))))))

(defn -main [& args] ; & creates a list of var-args
  (if (seq args)
    ; Foreach arg, print the arg...
    (doseq [arg args]
      
      (msectime (let [[check-result evidence-list] (linear-check-edn arg)]
                  (print arg)
                  (print "|"))))

    ; Handle failure however here
    (throw (Exception. "Must have at least one argument!"))))