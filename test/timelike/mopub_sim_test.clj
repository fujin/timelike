(ns timelike.mopub-sim-test
  (:refer-clojure :exclude [time future])
  (:use clojure.test
        [clojure.pprint :only [pprint]]
        timelike.scheduler
        timelike.node
        [incanter.stats :only [quantile]]))

(defn linesep
  [f]
  (f)
  (println))

(defn reset-test!
  [f]
  (reset-scheduler!)
  (f)
  (when-not (zero? @all-threads)
    (await-completion))
  (reset-scheduler!))

(use-fixtures :each reset-test!)

(defn pstats
  "Print statistics. We examing only the middle half of the request set, to
  avoid measuring ramp-up and draining dynamics."
  [reqs]
  (println)
  (let [n          (count reqs)
        reqs       (->> reqs
                        (drop (/ n 4))
                        (take (/ n 2)))
        latencies  (map latency reqs)
        response-rate (response-rate reqs)
        request-rate  (request-rate reqs)
        [q0 q5 q95 q99 q1] (quantile latencies :probs [0 0.5 0.95 0.99 1])]
    (println "Total reqs:      " n)
    (println "Selected reqs:   " (count reqs))
    (println "Successful frac: " (float (/ (count (remove error? reqs))
                                           (count reqs))))
    (println "Request rate:    " (float (* 1000 request-rate))  "reqs/s")
    (println "Response rate:   " (float (* 1000 response-rate)) "reqs/s")

    (println "Latency distribution:")
    (println "Min:    " q0)
    (println "Median: " q5)
    (println "95th %: " q95)
    (println "99th %: " q99)
    (println "Max:    " q1)))

(def n 1000000)
(def interval 1.5)
(def pool-size 215)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                 (load-poisson n interval req node))]
    (pstats @results)
    (println)))

(defn adserver
  "A multi-threaded libevent, request-queuing server, with a fixed per-request
  overhead plus an exponentially distributed time to process the request,
  connected by an extremely short LGA11 network cable."
  []
  (cable 2
         (queue-fixed-concurrency 24
                                  (delay-fixed 5
                                               (delay-exponential 100
                                                                  (server :libevent))))))

(defn faulty-adserver
  []
  (cable 2
         (queue-fixed-concurrency 24
                                  (faulty 20000 1000
                                          (delay-fixed 5
                                                       (delay-exponential 100
                                                                          (server :libevent)))))))
(defn faulty-adservers
  "A pool of n faulty adserver instances"
  [n]
  (pool n (faulty-adserver)))

(defn healthy-adservers
  "A pool of n healthy adserver instances"
  [n]
  (pool n (adserver)))

(defn faulty-lb
  [pool]
  (faulty 20000 1000
          (retry 3
                 (lb-min-conn :lb {:error-hold-time 1000}
                              pool))))

(defn healthy-lb
  [pool]
  (retry 3
         (lb-min-conn :lb {:error-hold-time 1000}
                      pool)))

(deftest ^:current production-adserver-current
  (test-node "Current: Retry -> 10x random adserver-proxy -> 215 Adserver processes"
             (let [adservers (pool pool-size (faulty-adserver))]
               (retry 3
                    (lb-random
                     (pool 10
                           (cable 5
                                  (healthy-lb
                                   adservers))))))))

(deftest ^:proposed production-adserver-proposed-faulty)
(test-node "Retry -> 10x random adserver_reverse_proxy -> 10x adserver faulty leastconn -> 23x faulty Adserver process"
           (retry 3
                  (lb-random
                   (pool 10
                         (cable 1
                                (faulty-lb
                                 (faulty-adservers 23)))))))

(deftest ^:proposed production-adserver-proposed-healthy)
(test-node "Retry -> 10x random adserver_reverse_proxy -> 10x adserver healthy leastconn -> 23x healthy Adserver process"
           (retry 3
                  (lb-random
                   (pool 10
                         (cable 1
                                (healthy-lb
                                        (healthy-adservers 23)))))))
