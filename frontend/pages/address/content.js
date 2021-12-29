import React, { useState, useEffect } from "react";
import axios from "axios";
import Image from "next/image";
import Card from "../share/card";
// import cityAvg from "../images/addr_cityAvg.jpg";

export default function Content() {
  // This part can be used for ML later
  //   const url = "http://127.0.0.1:8000";
  //   const sideUrl = url + "/Side";
  //   const [side, setSide] = useState([]);

  //   //   Get data needed for plot
  //   useEffect(() => {
  //     axios.get(sideUrl).then((res) => {
  //       setSide(res.data);
  //     });
  //   }, []);
  //   console.log(side);
  // const logo = require("../images/addr_cityAvg.jpg");
  // console.log(logo);

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 justify-center mt-4 mb-4 ml-8 mr-8">
      <Card
        imgPath="/addr_vis/cityAvg.jpg"
        title="Cities with most severe accidents"
        description="Only consider cities with more than 100 accident records. 
        None of the city on the list have huge population. 
        A number of them are known for tourism and natural resorts, 
        so the severity might be caused by drivers not familiar with the road 
        and bad road condition.
        "
      />

      <Card
        imgPath="/addr_vis/cityCount.jpg"
        title="Cities with most number of accident"
        description="This plot contains cities with most records of car accidents with a color scale mapped by average severity.
        Most of them are large cities, and LA's number is almost as twice as the second city Charlotte.
        "
      />
      <Card
        imgPath="/addr_vis/stateCount.jpg"
        title="10 States with most accidents"
        description="This plot sorts the most records of car accidents by states, and as we can see that
        California, which is one the biggest state by population and area, have accident numbers far greater than 
        other states."
      />
      <Card
        imgPath="/addr_vis/sideSeverityCount.jpg"
        title="Accidents vs Side of Road"
        description="Different accident severities aggregated by relative side of the street where the accident happens.
        Right side has a lot more accident than the left side, 
        but for the most severe accidents, left side's proportion increases significantly."
      />

      <Card
        imgPath="/addr_vis/timezone.jpg"
        title="Accidents vs Time Zone"
        description="Average Severity do not vary siginificantly across timezone, but East and West timezone take up
        more than 70% of the total accidents."
      />

      <Card
        imgPath="/addr_vis/dange_road.jpg"
        title="Most Accidents Roads"
        description="The top ten roads with the most accidents."
      />
    </div>
  );
}
