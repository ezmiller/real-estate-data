(ns real-estate-data.nyc.prep
  (:require
   [tablecloth.api :as tc]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [scicloj.clay.v2.api :as clay]
   [scicloj.kindly.v3.api :as kindly]
   [scicloj.kindly.v3.kind :as kind]))


#_(clay/start!)

(defn fix-column-names [s]
  (comp keyword s/lower-case #(s/replace % #"\s" "-")))

(let [directory (io/file "data/nyc-annual-sales-data")
      files (file-seq directory)
      csv-files (filter #(-> %
                         .getName
                         (clojure.string/split #"\.")
                         last
                         (= "csv"))
                        files)]
  (def ds
    (reduce (fn [final csv-file]
              (tc/concat final (tc/dataset csv-file
                                           {:key-fn (comp keyword
                                                          #(s/replace % #"\s" "-")
                                                          s/lower-case)})))
            (tc/dataset)
            csv-files)))

(kind/pprint
 (tc/head ds))

(kind/pprint
 (tc/info ds))

(tc/write-csv! ds "data/nyc-annual-sales--merged.csv")

