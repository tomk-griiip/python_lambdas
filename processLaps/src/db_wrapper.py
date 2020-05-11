from api_wrapper import ApiWrapper
from griiip_const import net
from griiip_exeptions import RunDataException, ApiException
from lambda_utils import environ


class DbMeta(type):
    """
    Db meta class to implement mandatory method in any type of class
    that should implement the process laps db management class
    """
    def __instancecheck__(self, instance):
        return self.__subclasscheck__(type(instance))

    def __subclasscheck__(self, subclass):
        return (hasattr(subclass, 'retrieveLapRunDataLapQuads') and
                callable(subclass.retrieveLapRunDataLapQuads) and
                hasattr(subclass, 'updateDriverLap') and
                callable(subclass.updateDriverLap))


class Idb(metaclass=DbMeta):
    """This interface is used for concrete classes to inherit from.
    There is no need to define the DbMeta methods as any class
    as they are implicitly made available via .__subclasscheck__().
    """

    pass


class DbApi:
    apiWrapper = None

    def _init__(self, *, api_url, api_key):

        self.apiWrapper = ApiWrapper(api_address=api_url, api_key=api_key)

    def retrieveLapRunDataLapQuads(self, lapId: str, limit: int, page: int) -> []:
        """
        function that get all the runData of the lap By lapId
        from 'driverlapsrundata' Table in RDS
        Parameters
        ----------
        lapId
        limit
        page

        Returns
        -------
        array of type RunDataRow each object in the array is one record
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

    def updateDriverLap(self, columns_to_update: {}, lap_name: str) -> bool:
        """
        Parameters
        ----------
        columns_to_update column withe their value to be update
        lap_name the lap to update

        Returns
        -------
        True for SUCCESS and False for FAILURE

        """
        try:
            res = self.apiWrapper.put(net.UPDATE_DRIVER_LAP_URL, json={**columns_to_update, "lapName": lap_name})

        except ApiException as api_e:
            print(f"db Api Exception : {api_e}")
            return net.FAILURE

        except Exception as e:
            print(f"DB Exception : {e}")
            return net.FAILURE

        if res.status_code == net.OK:
            return net.SUCCESS
        else:
            return net.FAILURE  # Consider raising exception instead


db = DbApi(api_url=environ('griiip_api_url'), api_key=environ('griiip_api_key'))
