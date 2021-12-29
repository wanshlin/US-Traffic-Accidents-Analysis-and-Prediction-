import React, { useState, useEffect } from "react";
import Card from "../share/card";
// import cityAvg from "../images/addr_cityAvg.jpg";

export default function Content() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 justify-center mt-4 mb-4 ml-6 mr-6">
      <Card
        imgPath="/weather_vis/Density_of_Accident_Severity_in_Pressure_in__Visibility_mi__and__Wind_Speed_mph_.png"
        title="Density of Accident Severity with Pressure(inch) Visibility(miles) and Wind Speed(mph)"
        description="There is not much of distinctions in wind speed and visibility figures. While the differences in pressure figure implies that level 1 accident would occur in a greater range of pressure than the others."
      />

      <Card
        imgPath="/weather_vis/Density_of_Accident_Severity_in_Temperature_F__and_Humidity___.png"
        title="Density of Accident Severity with Temperature(F) and Humidity"
        description="A serious accident is more likely to occur in low temperature and high humidity in comparison with other temperature and humidity conditions."
      />
      <Card
        imgPath="/weather_vis/Proportions_of_Accident_Severity_in_Various_Weather_Conditions.png"
        title="Proportions of Accident Severity in Various Weather Conditions"
        description="The proportion of level 3 accidents increases as weather changes from sand & dust (4.3%) to clear & cloiudy (10.8%) to rain (11.2%) to fog(14.1%) to storm(16.2%) to snow (17.2%)."
      />

      <Card
        imgPath="/weather_vis/Proportions_of_Accident_Severity_in_Various_Wind_Directions.png"
        title="Proportions of Accident Severity in Various Wind Directions"
        description="Having winds in any directions would increase the proportions of having a serious car accident when it compares with the Clam air."
      />
    </div>
  );
}
