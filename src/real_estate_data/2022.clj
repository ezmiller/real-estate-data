(ns real-estate-data.2022
  (:require [notespace.api :as notespace]
            [notespace.kinds :as kind]))


(require '[tablecloth.api :as tc]
         '[tech.v3.dataset :as ds])

(def rental-data (tc/dataset "data/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv"))

(def values-data (tc/dataset "data/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"))

^kind/dataset
(tc/head rental-data)

^kind/dataset
(tc/head values-data)

(tc/rows (ds/select-rows rental-data (range 2)) :as-maps)

;; date region region_id rental-value home-value

(def dates (tc/column-names
            (-> rental-data (tc/drop-columns
                             ["RegionID" "RegionName" "SizeRank"]))))


(def region-ids (-> rental-data (get "RegionID")))
(def region-names (-> rental-data (get "RegionName")))

(defn extract-ds-from-row [[region-id region-name _ & observations]
                           obs-name
                           dates]
  (tc/dataset {:region-id region-id
               :region-name region-name
               obs-name observations}))

(defn transform-ds [ds observation-name]
  (let [dates (tc/column-names
               (tc/drop-columns ds ["RegionID" "RegionName" "SizeRank"]))]
    (->> ds
         tc/rows
         (map #(extract-ds-from-row % observation-name dates))
         (reduce
          (fn [memo nextds]
            (tc/concat memo nextds))))))

(def transformed-rental-data
  (transform-ds rental-data :rental-price))

(def transformed-home-value-data
  (transform-ds values-data :home-value))


^kind/dataset
transformed-rental-data

^kind/dataset
transformed-home-value-data


