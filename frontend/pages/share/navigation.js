import React, { useState, useEffect } from "react";
import { useRouter } from "next/router";
import axios from "axios";

export default function Navigation() {
  const router = useRouter();
  const NoneSelectedClass =
    "text-center text-l block border border-blue-600 rounded py-2 px-4 bg-blue-600 hover:bg-blue-800 text-white hover:text-blue-200";
  const SelectedClass =
    "text-center text-l block border border-blue-600 rounded py-2 px-4 bg-blue-800 text-blue-300 cursor-not-allowed";
  return (
    <ul className="flex bg-blue-600 py-4">
      <li className="flex-1 mr-1 ml-4">
        <a
          className={
            router.pathname === "/" ? SelectedClass : NoneSelectedClass
          }
          href="/"
        >
          Address Visualization
        </a>
      </li>
      <li className="flex-1 mr-1">
        <a
          className={
            router.pathname === "/time/timePage"
              ? SelectedClass
              : NoneSelectedClass
          }
          href="/time/timePage"
        >
          Time Visualization
        </a>
      </li>
      <li className="text-center flex-1 mr-1">
        <a
          className={
            router.pathname === "/weather/weatherPage"
              ? SelectedClass
              : NoneSelectedClass
          }
          href="/weather/weatherPage"
        >
          Weather Visualization
        </a>
      </li>
      <li className="flex-1 mr-1">
        <a
          className={
            router.pathname === "/poi/poiPage"
              ? SelectedClass
              : NoneSelectedClass
          }
          href="/poi/poiPage"
        >
          POI Visualization
        </a>
      </li>
      <li className="flex-1 mr-4">
        <a
          className={
            router.pathname === "/machineLearning/mlPage"
              ? SelectedClass
              : NoneSelectedClass
          }
          href="/machineLearning/mlPage"
        >
          Machine Learinig
        </a>
      </li>
    </ul>
  );
}
