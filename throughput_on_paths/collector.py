import pandas as pd
import concurrent.futures
from elasticsearch.helpers import scan
import ipaddress
import urllib3
import helpers as hp
urllib3.disable_warnings()

class DataCollector:
    def __init__(self, period=None, bin_hours=4, ip_version=None, save_to_file=False, file_path="sample_ipv4.csv"):
        self.period = period
        self.bin_hours = bin_hours
        self.ip_version = ip_version  # Can be None, 'ipv4', or 'ipv6'
        self.save_to_file = save_to_file
        self.file_path = file_path


    def get_stats(self, time_range):
        """Fetch the combined traceroute and throughput dataset for the given time range."""
        results = []
        q = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "throughput_ts": {
                                    "format": "strict_date_optional_time",
                                    "lte": time_range[1],
                                    "gte": time_range[0]
                                }
                            }
                        },
                        # {
                        #     "term": {
                        #         "similarity_score": 1
                        #     }
                        # }
                    ]
                }
            }
        }
        data = scan(hp.es, index='routers', query=q)
        for item in data:
            results.append(item['_source'])

        if results:
            return pd.DataFrame(results)
        return pd.DataFrame()

    def collect_stats_in_parallel(self, time_ranges):
        """Collect stats using parallel processing across different time ranges."""
        stats_list = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.get_stats, time_range) for time_range in time_ranges]
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    if not data.empty:
                        stats_list.append(data)
                except Exception as e:
                    print(f"An error occurred: {e}")

        if stats_list:
            return pd.concat(stats_list, ignore_index=True)
        return pd.DataFrame()

    def is_private_ip(self, ip_addr):
        """Check if the IP address is private."""
        try:
            return ipaddress.ip_address(ip_addr).is_private
        except ValueError:
            return False

    def process_data(self, df):
        """Process and filter the data, depending on the IP version and other criteria."""
        # Filter based on IP version if provided
        if self.ip_version == 'ipv6':
            df = df[df['ipv6'] == 1].copy()
        elif self.ip_version == 'ipv4':
            df = df[df['ipv6'] == 0].copy()

        # Drop rows with NaN values
        df = df.dropna()

         # Ensure throughput_ts is converted to datetime
        df['throughput_ts'] = pd.to_datetime(df['throughput_ts'], errors='coerce')  # Convert to datetime, handle errors

        # Processing timestamps
        df['timestamp'] = df['throughput_ts'].astype('int64') // 10**9  # Convert to Unix timestamp


        # Apply private IP detection
        df['private'] = df['router'].apply(self.is_private_ip)

        # Round timestamps
        df['rounded_throughput_ts'] = df['throughput_ts'].dt.floor('2H')

        # Convert boolean columns to integers
        bool_columns = ['path_complete', 'private', 'destination_reached', 'stable']
        df[bool_columns] = df[bool_columns].astype(int)

        return df

    def run(self):
        """Main function to collect and process the data."""
        if self.period:
            # Prepare time ranges
            time_ranges = hp.split_time_period(self.period[0], self.period[1], bin_hours=self.bin_hours)

            # Collect the stats in parallel
            df = self.collect_stats_in_parallel(time_ranges)

            # Filter and process the data
            df = self.process_data(df)

            # Save to file if required
            if self.save_to_file:
                df.to_csv(self.file_path, index=False)
            else:
                return df

        else:
            # If no period is specified, load data from file if exists
            df = pd.read_csv(self.file_path)
            df = self.process_data(df)
            return df


# if __name__ == "__main__":
#     period = ['2024-08-01T06:22:19.000Z', '2024-08-21T06:22:19.000Z']
#     collector = DataCollector(period=period, ip_version='ipv4', save_to_file=True)
#     collector.run()
