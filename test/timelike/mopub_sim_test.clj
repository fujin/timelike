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

(defn adserver
  "Supposed to smell something like a libev based MPX process. Let's say it can handle 100 concurrent requests."
  []
  (cable 1
         (queue-fixed-concurrency 100
                                  (delay-fixed 5
                                               (delay-exponential 40
                                                                  (server :libevent))))))

(defn faulty-adserver
  "Supposed to smell something like a libev based MPX process. Let's say it can handle 100 concurrent requests."
  []
  (cable 1
         (faulty 20000 1000
                 (queue-fixed-concurrency 100
                                          (delay-fixed 5
                                                       (delay-exponential 40
                                                                          (server :libevent)))))))

(defn marketplace
  "Supposed to smell something like a libev based MPX process. Let's say it can handle 100 concurrent requests."
  []
  (cable 1
         (queue-fixed-concurrency 100
                                  (delay-fixed 5
                                               (delay-exponential 300
                                                                  (server :libevent))))))
(defn faulty-marketplace
  "Supposed to smell something like a libev based MPX process. Let's say it can handle 100 concurrent requests."
  []
  (cable 1
         (faulty 20000 1000
                 (queue-fixed-concurrency 100
                                          (delay-fixed 5
                                                       (delay-exponential 300
                                                                          (server :libevent)))))))

(defn adservers
  "A pool of n adservers"
  [n]
  (pool n (adserver)))

(defn faulty-adservers
  [n]
  (pool n (faulty-adserver)))

(defn marketplaces
  "A pool of n marketplaces"
  [n]
  (pool n (marketplace)))

(defn faulty-marketplaces
  [n]
  (pool n (faulty-marketplace)))


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
                            (adservers pool-size)))))))

(defn adserver-leastconn-test
  [n]
  (test-node (str pool-size " adservers with " n " leastconn routers")
             (retry 3
                    (lb-random
                     (pool n
                           (lb-min-conn :lb {:error-hold-time 1000}
                                        (adservers pool-size)))))))

(defn adserver-faulty-random-test
  [n]
  (test-node (str pool-size " adservers @ 90% availability with " n " random routers")
             (retry 3
                    (lb-random
                     (pool n
                           (lb-random
                            (faulty-adservers pool-size)))))))

(defn marketplace-faulty-leastconn-test
  [proxies instances processes]
  (test-node
   (str proxies
        " lc marketplace proxies in front of one (shared) pool of "
        (* instances processes)
        " processes")
   (retry 3
          (lb-rr ;; simulating DNS RR
           (pool proxies
                 (cable 5
                        (faulty-lb
                         (faulty-marketplaces (* instances processes)))))))))

(defn adserver-faulty-leastconn-test
  [proxies instances processes]
  (test-node
   (str proxies
        " lc adserver loadbalancers in front of one (shared) pool of "
        (* instances processes)
        " processes")
   (retry 3
          (lb-rr ;; simulating DNS RR
           (pool proxies
                 (cable 5
                        (faulty-lb
                         (faulty-adservers (* instances processes)))))))))


(defn proposed-adserver-faulty-test
  [proxies instances processes]
  (test-node
   (str proxies
        " random adserver loadbalancers in front of "
        instances
        " instances, each w/ distinct pool of least-conn "
        processes
        " processes over loopback reverse proxy")
   (retry 3
          (lb-rr ;; via anycast IPv4 with any luck
           (pool proxies
                 (cable 5
                        (faulty-lb
                         (faulty-adservers 23))))))))

(defn proposed-marketplace-faulty-test
  [proxies instances processes]
  (test-node
   (str proxies
        " random marketplace loadbalancers in front of "
        instances
        " instances, each w/ distinct pool of least-conn "
        processes
        " processes over loopback reverse proxy")
   (retry 3
          (lb-rr ;; via anycast IPv4 with any luck
           (pool proxies
                 (cable 5
                        (faulty-lb
                         (faulty-marketplaces 23))))))))


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

(deftest ^:leastconn marketplace-faulty-leastconn
  (marketplace-faulty-leastconn-test proxies instances processes))

(deftest ^:proposed ^:hybrid marketplace-hybrid
  (proposed-marketplace-faulty-test proxies instances processes))

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
