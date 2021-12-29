import React, { useState, useEffect } from "react";
import Card from "../share/card";
import MlCard from "./mlCard";
// import cityAvg from "../images/addr_cityAvg.jpg";

export default function Content() {
  return (
    <div>
      <div className="text-5xl font-extrabold px-40 text-center my-3">
        <span className="bg-clip-text text-transparent bg-gradient-to-r from-green-400 to-blue-500">
          This is the exciting page! Welcome to the machine learning part!
        </span>
      </div>

      {/* <div class="content flex py-2">
        <img class="w-48 h-48" src="#" alt="" />
        <div class="item-body px-2 ">This is a normal card</div>
      </div> */}

      <div className="grid grid-cols-1 gap-4 justify-center my-4 ml-6 mr-6 pb-10">
        <MlCard
          imgPath="/predict_vis/dt_new.png"
          title="Predicted Result of Decision Tree (after undersampling)"
          description="This is a lot more balanced and accurate performance compared with previous models"
        />
        <MlCard
          imgPath="/predict_vis/dt_new-1.png"
          title="
          Actual Severity in a Map View"
          description="The map displays geographic coordinates of accidents with their actual severity. Most of the accidents concentrate on the East and West Coasts, 
          and the west coasts are populated with desne blue points, indicating smaller average severity compared with other regions"
        />
        <MlCard
          imgPath="/predict_vis/dt_new-2.png"
          title="Predicted Severity in the Map View"
          description="The map shows geographic coordinates of accidents with their actual severity. The result resembles the actual data with some key characteristics:
          the east part are populated with mostly severity 3 accidents, 
          with a cluster of desen minor accidents in the North East and Middle Notrh, possibly due to the cold weather there.
          "
        />
      </div>
    </div>
  );
}
