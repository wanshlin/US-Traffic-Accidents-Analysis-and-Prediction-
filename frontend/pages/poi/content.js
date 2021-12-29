import React, { useState, useEffect } from "react";
import Card from "../share/card";

export default function Content() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 justify-center mt-4 mb-4 mx-8">
      <div className="rounded overflow-hidden shadow-lg ">
        <div className="flex h-4/6">
          <img
            className="flex-1 w-6/12"
            src={"/poi_vis/2_poiCount.png"}
            alt="CityCount"
          />
          <img
            className="flex-1 w-6/12"
            src={"/poi_vis/2_poiPer.png"}
            alt="CityCount"
          />
        </div>

        <div className="px-6 py-4">
          <div className="font-bold text-xl mb-2">POI Count and Proportion</div>
          <p className="text-gray-700 text-base">the number and percentage of points of interest and no points of interest near the accident locations</p>
        </div>
      </div>

      <Card
        imgPath="/poi_vis/3_poi.png"
        title="Different Severity with/without POI"
        description="The number of accidents with and without points of interest nearby for each severity level"
      />

      <Card
        imgPath="/poi_vis/4_poiPer.png"
        title="POI Percentage Distribution"
        description="The proportion of accidents with different types of points of interest in the surrounding area"
      />

      <Card
        imgPath="/poi_vis/4_poiType.png"
        title="Number of Accidents with different POI Types"
        description="The number of accidents with different types of points of interest in the surrounding area"
      />

      <Card
        imgPath="/poi_vis/5_poiSev.png"
        title="Severity Level with POI"
        description="The number of accidents grouped by their nearby points of interest and severity levels"
      />
    </div>
  );
}
