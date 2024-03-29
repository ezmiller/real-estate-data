^:kindly/hide-code?
(ns real-estate-data.nyc.explore
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as fun]
            [clojure.string :as s]
            [scicloj.clay.v2.api :as clay]
            [scicloj.kindly-default.v1.api :as kindly-default]
            [scicloj.kindly.v3.api :as kindly]
            [scicloj.kindly.v3.kind :as kind]
            [scicloj.kindly.v3.kindness :as kindness]
            [scicloj.viz.api :as viz]
            [aerial.hanami.templates :as ht]
            [aerial.hanami.common :as hc]
            [tablecloth.time.api :as tct]
            ))

^:kindly/hide-code?
(kindly-default/setup!)
(clay/start!)

;; Here is our dataset 
(def ds (tc/dataset "./data/nyc-annual-sales--merged.csv"
                    {:key-fn keyword}))

(tc/row-count ds)

(tc/head ds)

;; Let's look at general descriptive statistics
(tc/info ds)

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

(tc/info cleaned-ds)

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
    viz/data
    (viz/x :sale-price)
    (viz/type [:histogram {:bin-count 30}])
    viz/viz)

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

(-> cleaned-ds
    (tc/group-by [:neighborhood :building-class-category])
    (tc/aggregate
     {:sale-price (comp fun/mean :sale-price)})
    (tc/select-rows
     (fn [row]
       (let [category (:building-class-category row)]
         (or (= category single-family)
             (= category two-family)
             (= category three-family)))))
    (viz/data)
    (viz/type ht/grouped-bar-chart)
    (viz/viz {:HEIGHT 800
              :Y :neighborhood :YTYPE "nominal"
              :YSORT "x"
              :X :sale-price
              :COLUMN :building-class-category :COLTYPE "ordinal"}))


;; rent to price


;; https://www.rentcafe.com/average-rent-market-trends/us/ny/brooklyn/

(def ds-rent-to-price
  (-> cleaned-ds
      (tc/add-column :avg-rent 3194)
      (tc/map-columns :rent-to-price
                      :float32
                      [:sale-price :avg-rent]
                      (fn [price avg-rent]
                        (* 100 (/ avg-rent price))))))


(tc/head ds-rent-to-price)

(-> ds-rent-to-price
    (tc/select-rows (comp #(= single-family %) :building-class-category))
    (tc/select-rows (comp #(< % 1) :rent-to-price))
    (tc/group-by [:neighborhood])
    (tc/aggregate
     {:mean-rent-to-price (comp fun/mean :rent-to-price)})
    (viz/data)
    (viz/type ht/bar-chart)
    (viz/viz {:Y :neighborhood 
              :YTYPE "nominal"
              :YSORT "x"
              :X :mean-rent-to-price
              :HEIGHT 800})
    )


;; time dimension

(-> cleaned-ds
    viz/data
    (viz/type ht/line-chart)
    (viz/viz {:X :sale-date :XTYPE "temporal"
              :Y :sale-price}))


(-> cleaned-ds
    (tct/adjust-frequency tct/->months-end)
    (tc/aggregate {:median-sale-price (comp fun/median :sale-price)})
    viz/data
    (viz/type ht/line-chart)
    (viz/viz {:X :sale-date :XTYPE "temporal"
              :Y :median-sale-price}))
