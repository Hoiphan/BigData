document.addEventListener('DOMContentLoaded', () => {
    const map = L.map('map').setView([10.80, 106.64], 13);
    const roadLayers = {};

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);


    function aggregateFeatures(geojson) {
        const aggregatedFeatures = {};
        let i = 0;
        geojson.features.forEach(feature => {
            const roadName = feature.properties.name;
            const geometry = feature.geometry;
            const length = feature.properties.length || 0;

            if (roadName && roadName !== 'nan') {
                if (!aggregatedFeatures[roadName]) {
                    aggregatedFeatures[roadName] = {
                        type: 'Feature',
                        properties: {
                            name: roadName,
                            id: "LOC_" + i,
                            total_length: 0
                        },
                        geometry: {
                            type: 'MultiLineString',
                            coordinates: []
                        }
                    };
                    i++;
                }

                if (geometry.type === 'LineString') {
                    aggregatedFeatures[roadName].geometry.coordinates.push(geometry.coordinates);
                } else if (geometry.type === 'MultiLineString') {
                    aggregatedFeatures[roadName].geometry.coordinates.push(...geometry.coordinates);
                }
                aggregatedFeatures[roadName].properties.total_length += length;
            }
        });

        return {
            type: 'FeatureCollection',
            features: Object.values(aggregatedFeatures)
        };
    }

    function updateRoadColor(locId, statusCode) {
        const layer = roadLayers[locId];
        if (layer) {
            let color = '#0000FF'; // Default color
            switch (statusCode) {
                case 0:
                    color = '#00FF00'; // Green
                    break;
                case 1:
                    color = '#FFA500'; // Orange
                    break;
                case 2:
                    color = '#FF0000'; // Red
                    break;
            }
            layer.setStyle({ color: color });
        }
    }

    fetch('./data/output_map.geojson')
        .then(response => response.json())
        .then(data => {
            const aggregatedData = aggregateFeatures(data);

            const geoJsonLayer = L.geoJSON(aggregatedData, {
                style: {
                        color: "#0000FF",
                        weight: 5,
                        opacity: 0.75
                },
                onEachFeature: (feature, layer) => {
                    if (feature.properties && feature.properties.name) {
                        const roadName = feature.properties.name;
                        const totalLength = (feature.properties.total_length / 1000).toFixed(2);
                        layer.bindPopup(`<b>${roadName}</b><br>Total Length: ${totalLength} km`);
                        roadLayers[feature.properties.id] = layer;
                    }
                }
            }).addTo(map);

            if (geoJsonLayer.getBounds().isValid()) {
                map.fitBounds(geoJsonLayer.getBounds());
            }
        })
        .catch(error => console.error('Error loading or processing GeoJSON data:', error));

    const socket = new WebSocket('ws://localhost:3000');

    socket.onopen = () => {
        console.log('WebSocket connection established');
    };

    socket.onmessage = event => {
        const message = JSON.parse(event.data);
        console.log('Kafka Message:', message);
        if(message.sensor_id && message.label){
            updateRoadColor(message.sensor_id, message.label);
        }
    };

    socket.onerror = error => {
        console.error('WebSocket error:', error);
    };

    socket.onclose = () => {
        console.log('WebSocket connection closed');
    };
});
