import React, { useState, useEffect } from "react";
import "./App.css";
import FilterUsers from "./components/FilterUsers";
import json_string from "./data/matrix_intersection_05.json";

function App() {
	const [data, setData] = useState([]);
	
	useEffect(() => {
		const data = JSON.parse(json_string);
		const sampledData = data;

		setData(sampledData);
	}, []);

	return (
		<div className="App">
			<div>
				<h1>Network Visualization</h1>
				<FilterUsers userMatrix={data} />
			</div>
		</div>
	);
}

export default App;
