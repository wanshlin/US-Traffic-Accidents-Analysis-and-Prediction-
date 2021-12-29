import React from "react";

function MlCard(props) {
  return (
    <div className="rounded-large overflow-hidden shadow-lg bg-blue-200 content flex h-96">
      <img className="w-auto h-full" src={props.imgPath} alt="CityAvg" />
      <div className="px-6 py-4">
        <div className="font-bold text-3xl mb-2">{props.title}</div>
        <p className="text-gray-700 text-2xl item-body px-2">
          {props.description}
        </p>
      </div>
    </div>
  );
}

export default MlCard;
