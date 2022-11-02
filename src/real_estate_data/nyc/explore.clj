^:kindly/hide-code?

(ns real-estate-data.nyc.explore
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as fun]
            [clojure.string :as s]
            [scicloj.clay.v2.api :as clay]
            [scicloj.kindly.v3.api :as kindly]
            [scicloj.kindly.v3.kind :as kind]
            [scicloj.viz.api :as viz]
            [aerial.hanami.templates :as ht]
            [aerial.hanami.common :as hc]
            ))

^:kind/hidden
(clay/start!)

;; Here is our dataset 
(def ds (tc/dataset "./data/nyc-annual-sales--merged.csv"
                    {:key-fn keyword}))

(kind/pprint
 (tc/head ds))


;; Let's look at general descriptive statistics
(kind/pprint
 (tc/info ds))

(defn clean-ds [ds]
  (-> ds
      (tc/drop-missing :sale-price)
      (tc/replace-missing :building-class-category :value "UNKOWN")
      (tc/update-columns {:sale-price (partial map #(s/replace % #"," ""))})
      (tc/convert-types {:sale-price :int64})
      (tc/drop-rows (comp #(= 0 %) :sale-price))
      ))


(def cleaned-ds
  (clean-ds ds))

(kind/pprint
 (tc/info cleaned-ds))

(-> cleaned-ds
    tc/info
    (tc/select-rows (comp #(= % :sale-price) :col-name)))


;; What are the set of building categories?
(-> cleaned-ds
    :building-class-category
    set
    sort)


(-> cleaned-ds
    :sale-price
    (->> (tech.v3.datatype.functional/percentiles [25 50 75]))
    )

(-> cleaned-ds
    (tc/select-columns [:sale-price])
    (tc/select-rows (comp #(< % 4000000) :sale-price))
    viz/data
    (viz/x :sale-price)
    (viz/type [:histogram {:bin-count 30}])
    viz/viz
    )

(defn histogram [ds x]
  (-> ds
      viz/data
      (viz/x x)
      (viz/type [:histogram {:bin-count 30}])
      viz/viz
      ))

(def single-family "01 ONE FAMILY DWELLINGS")
(def two-family "02 TWO FAMILY DWELLINGS")
(def three-family "03 THREE FAMILY DWELLINGS")


(-> cleaned-ds
    (tc/select-rows (comp #(= % single-family)
                          :building-class-category))
    (tc/select-rows (comp #(< % 4000000) :sale-price))
    (tc/select-columns [:sale-price])
    (histogram :sale-price))

(-> cleaned-ds
    (tc/select-rows (comp #(= % two-family)
                          :building-class-category))
    (tc/select-rows (comp #(< % 4000000) :sale-price))
    (tc/select-columns [:sale-price])
    (histogram :sale-price))

(-> cleaned-ds
    (tc/select-rows (comp #(= % three-family)
                          :building-class-category))
    (tc/select-rows (comp #(< % 4000000) :sale-price))
    (tc/select-columns [:sale-price])
    (histogram :sale-price))

;; by neighborhood

(-> cleaned-ds
    :neighborhood
    set
    count)

(-> cleaned-ds
    (tc/group-by [:neighborhood :building-class-category])
    (tc/aggregate
     {:sale-price (comp fun/mean :sale-price)})
    (viz/data)
    (viz/type ht/bar-chart)
    (viz/viz {:Y :neighborhood 
              :YTYPE "nominal"
              :YSORT "x"
              :X :sale-price 
              :HEIGHT 800
              :TRANSFORM [{:filter {:field :building-class-category
                                    :equal single-family}}]})
    )
