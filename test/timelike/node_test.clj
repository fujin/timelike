(ns timelike.node-test
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

(deftest retry-test
         (thread
           (is (= ((retry 3 (fn [r] (sleep 1) (conj r (error)))) (req))
                  [{:time 0}
                   {:time 1 :error true}
                   {:time 1 :retry 1}
                   {:time 2 :error true}
                   {:time 2 :retry 2}
                   {:time 3 :error true}]))))
