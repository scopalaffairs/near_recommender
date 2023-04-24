import React, { useEffect, useState } from "react";
import "./FilterUsers.css";
import NetworkVisualization from "./NetworkVisualization";

const DEBOUNCE_DELAY = 300; // milliseconds
const UserFilter = ({ userMatrix }) => {
	const [filterValue, setFilterValue] = useState("");
	const [filteredUsers, setFilteredUsers] = useState([]);

	useEffect(() => {
		if (filterValue.length >= 3) {
			const filteredData = userMatrix.filter((item) =>
				item.user.includes(filterValue)
			);

			setFilteredUsers(filteredData);
		} else {
			setFilteredUsers([]);
		}
	}, [filterValue, userMatrix]);

	const handleInputChange = debounce((event) => {
		setFilterValue(event.target.value);
	}, DEBOUNCE_DELAY);

	return (
		<div>
			<label htmlFor="user-filter">Filter users:</label>
			<input
				type="text"
				id="user-filter"
				name="user-filter"
				onChange={handleInputChange}
			/>
			{filteredUsers.length > 0 && (
				<ul>
					{filteredUsers.map((user) => (
						<li key={user.user}>
							{user.user}
							<ul>
								{user.recommended_users.map(
									(recommendedUser) => (
										<li key={recommendedUser.node}>
											{recommendedUser.node} | {recommendedUser.authority}
										</li>
									)
								)}
							</ul>
						</li>
					))}
				</ul>
			)}
 <NetworkVisualization data={filteredUsers}/>
		</div>
	);
};

function debounce(func, delay) {
	let timeoutId;
	return function (...args) {
		if (timeoutId) {
			clearTimeout(timeoutId);
		}
		timeoutId = setTimeout(() => {
			func.apply(this, args);
		}, delay);
	};
}

export default UserFilter;
