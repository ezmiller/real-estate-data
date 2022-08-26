(ns real-estate-data.scratch
  (:require [scicloj.clay.v1.api :as clay]
            [scicloj.clay.v1.tools :as tools]
            [nextjournal.clerk :as clerk])
  )

(clay/start! {:tools [tools/clerk
                      tools/portal]})

(comment
  (clay/restart! {:tools [tools/clerk
                          tools/portal]})

  (clerk/clear-cache!)
  (clerk/halt!)
  ,)

#_(require '[tablecloth.api :as tc]
         '[tablecloth.time.api :as tct]
         '[tech.v3.dataset :as tds]
         '[tech.v3.datatype.functional :as fun])

;; (clerk/set-viewers!
;;  [{:pred tc/dataset?
;;    :transform-fn #(clerk/table {:head (tds/column-names %)
;;                                 :rows (tds/rowvecs %)})}])


(def rental-data
  (tc/dataset "data/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv"))

(tds/rowvecs rental-data)


(apply + (range 10))

(+ 10 10)

