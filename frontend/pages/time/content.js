import React, { useState, useEffect } from "react";
import Card from "../share/card";
// import cityAvg from "../images/addr_cityAvg.jpg";

export default function Content() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 justify-center mt-4 mb-4 ml-6 mr-6">
      <Card
        imgPath="/time_vis/hour.jpg"
        title="Accident Distribution by Hour"
        description="The number of hourly traffic accidents shows a significant increase in the number of 
        accidents during rush hours."
      />

      <Card
        imgPath="/time_vis/projectMonth.jpg"
        title="Accident Distribution by Month"
        description="We can see the increase in traffic accidents in winter."
      />
      <Card
        imgPath="/time_vis/week_day.jpg"
        title="
      Accidents with Week Day Distribution"
        description="This graph shows that there are more accidents on most weekdays and the least accidents on Mondays and Sundays."
      />

      <Card
        imgPath="/time_vis/red_blue.png"
        title="Red State vs Blue State"
        description=" The number of traffic accidents per capita in blue states (CA, NY, IL, MA, NJ) 
        is twice that in red states (OH, TX, TN, IN, MO)."
      />

      <Card
        imgPath="/time_vis/south_north.jpg"
        title="Accidents VS Lattitude"
        description="Traffic accidents in areas with latitude > 40 rise significantly in winter, 
        while the number of accidents in the south with latitude < 35 does not vary significantly across seasons."
      />

      <Card
        imgPath="/time_vis/most_day_city.png"
        title="The Date and City with Most Accidents"
        description="The graph shows which days had the most traffic accidents in the top 10 cities with the most traffic accidents."
      />

      <Card
        imgPath="/time_vis/LA_1223_weather.png"
        title="
       LA Accidents with Weather Data"
        description="From the graph above we can see that Los Angeles had the highest number of traffic accidents on December 23, 2019. 
        As you can see from this graph, the weather conditions were not good that day."
      />
    </div>
  );
}
