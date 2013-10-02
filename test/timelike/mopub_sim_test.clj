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

(def n 100000)
(def interval 1)
(def pool-size 215)
(def proxies 8)
(def instances 215)
(def processes 23)

(defn test-node
  [name node]
  (println name)
  (let [results (future*
                 (load-poisson n interval req node))]
    (pstats @results)
    (println)))

(defn dyno
  "A singlethreaded, request-queuing server, with a fixed per-request
  overhead plus an exponentially distributed time to process the request,
  connected by a short network cable."
  []
  (cable 2
         (queue-exclusive
          (delay-fixed 20
                       (delay-exponential 100
                                          (server :rails))))))

(defn faulty-dyno
  "Like a dyno, but only 90% available."
  []
  (cable 2
         (faulty 20000 1000
                 (queue-exclusive
                  (delay-fixed 20
                               (delay-exponential 100
                                                  (server :rails)))))))

(defn dynos
  "A pool of n dynos"
  [n]
  (pool n (dyno)))

(defn faulty-dynos
  [n]
  (pool n (faulty-dyno)))

(defn faulty-lb
  [pool]
  (faulty 20000 1000
          (retry 3
                 (lb-min-conn :lb {:error-hold-time 1000}
                              pool))))
(defn adserver-random-test
  [n]
  (test-node (str pool-size " adservers with " n " random routers")
             (retry 3
                    (lb-random
                     (pool n
                           (lb-random
                            (dynos pool-size)))))))

(defn adserver-leastconn-test
  [n]
  (test-node (str pool-size " adservers with " n " leastconn routers")
             (retry 3
                    (lb-random
                     (pool n
                           (lb-min-conn :lb {:error-hold-time 1000}
                                        (dynos pool-size)))))))

(defn adserver-faulty-random-test
  [n]
  (test-node (str pool-size " adservers @ 90% availability with " n " random routers")
             (retry 3
                    (lb-random
                     (pool n
                           (lb-random
                            (faulty-dynos pool-size)))))))

(defn adserver-faulty-leastconn-test
  [proxies instances processes]
  (test-node
   (str proxies
        " lc proxies in front of one pool of "
        (* instances processes)
        " processes")
     (retry 3
            (lb-rr ;; simulating DNS RR
             (pool proxies
                   (cable 5
                          (lb-min-conn :lb {:error-hold-time 1000}
                                       (faulty-dynos (* instances processes)))))))))


(defn proposed-adserver-faulty-test
  [proxies instances processes]
  (test-node
   (str proxies
        " random proxies in front of "
        instances
        " instances, each w/ distinct pool of least-conn "
        processes
        " processes over loopback")
   (retry 3
          (lb-rr ;; via anycast IPv4 with any luck
           (pool proxies
                 (cable 5
                        (lb-min-conn :lb {:error-hold-time 1000}
                                     (faulty-dynos 23))))))))


;; (deftest ^:random adserver-random-8
;;   (adserver-random-test 8))

;; (deftest ^:leastconn adserver-leastconn-8
;;   (adserver-leastconn-test 8))

;; (deftest ^:random adserver-faulty-random-8
;;   (adserver-faulty-random-test 8))

(deftest ^:leastconn adserver-faulty-leastconn
  (adserver-faulty-leastconn-test proxies instances processes))

(deftest ^:proposed ^:hybrid adserver-hybrid
  (proposed-adserver-faulty-test proxies instances processes))

;; (deftest ^:current production-adserver-current-faulty
;;   (test-node "Current: Retry -> 10x random adserver-proxy -> 215 faulty Adserver processes"
;;              (let [adservers (pool pool-size (faulty-adserver))]
;;                (retry 3
;;                       (lb-random
;;                        (pool 10
;;                              (cable 5
;;                                     (adservers))))))))

;; (deftest ^:proposed production-adserver-proposed-faulty
;;   (test-node "Retry -> 10x random adserver_reverse_proxy -> 10x adserver faulty leastconn -> 23x faulty Adserver process"
;;              (retry 3
;;                     (lb-random
;;                      (pool 10
;;                            (cable 1
;;                                   (faulty-lb
;;                                    (faulty-adservers 23))))))))

;; (deftest ^:proposed production-adserver-proposed-healthy
;;   (test-node "Retry -> 10x random adserver_reverse_proxy -> 10x adserver healthy leastconn -> 23x healthy Adserver process"
;;              (retry 3
;;                     (lb-random
;;                      (pool 10
;;                            (cable 1
;;                                   (healthy-lb
;;                                    (healthy-adservers 23))))))))
