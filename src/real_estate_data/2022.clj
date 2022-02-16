(ns real-estate-data.2022
  (:require [notespace.api :as notespace]
            [notespace.kinds :as kind]))


(require '[tablecloth.api :as tc]
         '[tablecloth.time.api :as tct]
         '[tech.v3.dataset :as ds]
         '[tech.v3.datatype.functional :as fun])

(def rental-data (tc/dataset "data/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv"))

(def values-data (tc/dataset "data/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"))

^kind/dataset
(tc/head rental-data)

^kind/dataset
(tc/head values-data)

(-> values-data
    (tc/select-rows (comp #(= % "New York, NY") #(get % "RegionName")))
    (get "2021-12-31")
    float)

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

(defn transform-rental-ds [ds]
  (let [dates (tc/column-names
               (tc/drop-columns ds ["RegionID" "RegionName" "SizeRank"]))]
    (->> ds
         tc/rows
         (map (fn [[region-id region-name size-rank & observations]]
                (tc/dataset {:date dates
                             :region-id region-id
                             :region-name region-name
                             :size-rank size-rank
                             :rent-price observations})))
         (reduce
          (fn [memo nextds]
            (tc/concat memo nextds))))))

(defn transform-home-value-ds [ds]
  (let [dates (tc/column-names
               (tc/drop-columns ds
                                ["RegionID" "SizeRank" "RegionName"
                                 "RegionType" "StateName"]))]
    (->> ds
         tc/rows
         (map (fn [[region-id
                    size-rank
                    region-name
                    region-type
                    state-name & observations]]
                (tc/dataset {:date dates
                             :region-id region-id
                             :size-rank size-rank
                             :region-name region-name
                             :region-type region-type
                             :state-name state-name
                             :home-value observations})))
         (reduce
          (fn [memo nextds]
            (tc/concat memo nextds))))))

(def transformed-rental-data
  (-> rental-data
      (transform-rental-ds)
      (tc/add-column :date
                     (fn [ds]
                       (->> (:date ds)
                            (map #(apply str % "-01"))
                            (map tct/string->time)
                            (map tct/->months-end))))))

(def transformed-home-value-data
  (-> values-data
      (transform-home-value-ds)
      (ds/column-cast :date :packed-local-date)
      (tc/add-column :home-value-last-year #(fun/shift (:home-value %) 12))
      (tc/add-column :appreciation
                    (fn [ds]
                      (fun/* 100
                              (fun// (fun/- (:home-value ds) (:home-value-last-year ds))
                                    (:home-value ds)))))
      (tc/drop-columns [:home-value-last-year])))


^kind/dataset
transformed-rental-data

^kind/dataset
transformed-home-value-data

^kind/dataset
(-> transformed-rental-data
    (tc/select-rows (comp #(= % "Seattle, WA") :region-name))
    (tct/adjust-frequency tct/->years-end {:include-columns [:region-name
                                                             :region-id]})
    (tc/aggregate {:mean-rent  #(fun/mean (% :rent-price))}))


^kind/dataset
(-> transformed-rental-data
    (tc/select-rows (comp #(= % 395078) :region-id))
    (tct/slice "2021-01-01" "2021-12-31"))

^kind/dataset
(-> transformed-home-value-data
    (tc/select-rows (comp #(= % 395078) :region-id))
    (tct/slice "2021-01-01" "2021-12-31"))

(def rtp-data
  (-> (tc/inner-join
       (-> transformed-home-value-data
           ;; (tc/select-rows (comp #(= % 395078) :region-id))
           (tct/slice "2021-01-01" "2021-12-31")
           (tc/select-columns [:date :region-id :region-name :home-value :appreciation]))
       (-> transformed-rental-data
           ;; (tc/select-rows (comp #(= % 395078) :region-id))
           (tct/slice "2021-01-01" "2021-12-31")
           (tc/select-columns [:date :region-id :rent-price]))
       [:date :region-id])
      (tc/add-column :rtp
                          (fn [ds] (fun/* 100
                                          (fun// (:rent-price ds)
                                                  (:home-value ds)))))
      (tc/drop-columns [:right.date :right.region-id])
      (tc/order-by [:region-id :date])))


^kind/dataset
rtp-data

^kind/dataset
(-> rtp-data
    (tc/select-columns [:date :region-name :rtp :appreciation])
    (tct/adjust-frequency tct/->years-end
                          {:include-columns [:date :region-name]
                           :ungroup? true})
    (tc/group-by [:region-name])
    (tc/aggregate {:mean-rtp #(fun/mean (:rtp %))
                   :mean-appreciation #(fun/mean (:appreciation %))})
    (tc/select-rows (comp #(> % 0.65) :mean-rtp))
    (tc/reorder-columns [:region-name :mean-rtp :mean-appreciation])
    (tc/order-by [:mean-appreciation :mean-rtp] :desc))

;; ^kind/dataset
;; (tc/select-rows
;;  rtp-data
;;  (comp #(re-find #"Seattle" %) :region-name))





