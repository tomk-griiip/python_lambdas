from api_wrapper import ApiWrapper
from griiip_const import net
from griiip_exeptions import RunDataException
from lambda_utils import environ


class DB:
    def __init__(self, **dbConfig):
        self.dbConfig = dbConfig
        self.subInit()

    def subInit(self):
        pass

    def conf(self, key: str):
        return self.dbConfig.get(key)


class DbApi(DB):
    apiWrapper = None

    def subInit(self):
        api_address = self.conf(key='api_url')
        api_key = self.conf(key='api_key')
        self.apiWrapper = ApiWrapper(api_address=api_address, api_key=api_key)

    def retrieveLapRunDataLapQuads(self, lapId: str, limit: int, page: int) -> []:
        """
        @retrieveLapRunData : function that get all the runData of the lap By lapId
        from 'driverlapsrundata' Table in RDS
        @:param lapId the lapId to get its all data from driverLapsRunData table
        @:return array of type RunDataRow each object in the array is one record
        of the lapRunData
        """

        payload = {'lapName': lapId, 'page': page, 'limit': limit}
        # call API to get runData
        runData: dict = self.apiWrapper.get(net.RUNDATA_URL, params=payload).json()['data']

        if len(runData) == 0:
            raise RunDataException

        # some times the first rows is mistaken distance data
        # and need to remove them from the run data ro
        def removed_first_bad_distance_rows() -> int:
            glitches, total_rows, g = 0, len(runData), 0
            for row_id in range(total_rows - 1):
                # In this case, the row 'distance' value is bigger then the next row 'distance' value.
                if runData[row_id]['distance'] > runData[row_id + 1]['distance']:
                    glitches += 1
                else:
                    break
            return glitches

        # the number of glitches in thr beginning of the lap
        num_dist_glit: int = removed_first_bad_distance_rows()
        if num_dist_glit > 0:
            print(f"FOUND {num_dist_glit} BAD ROWS FOR LAP {lapId}"
                  f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

        return runData[num_dist_glit:]  # remove the rows with the distance glitches in the


db = DbApi(api_url=environ('griiip_api_url'), api_key=environ('griiip_api_key'))
