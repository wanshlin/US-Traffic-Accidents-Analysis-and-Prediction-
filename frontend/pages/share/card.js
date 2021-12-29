import React from "react";

function Card(props) {
  return (
    <div className="rounded overflow-hidden shadow-lg bg-blue-200">
      <img className="w-full h-4/6" src={props.imgPath} alt="CityAvg" />
      <div className="px-6 py-4">
        <div className="font-bold text-xl mb-2">{props.title}</div>
        <p className="text-gray-700 text-base">{props.description}</p>
      </div>
    </div>
  );
}

export default Card;
