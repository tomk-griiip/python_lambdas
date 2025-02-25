from interfaces import Iclassifier
from griiip_exeptions import *

"""
classify laps according to predefine set of rules
"""


class RuleBaseClassifier(Iclassifier):
    lap, api = None, None

    """
    @:function classify return str classification of the lap 
    @:param lap LapBean object that represent lap 
    @:param api a class with function that represent missing data api from db
    pass inside **kwargs
    """

    def classify(self, lap, **kwargs) -> str:
        if 'api' not in kwargs:
            raise KwargsMissingArgException(function='RuleBaseClassifier.classify',
                                            missingArgs=['api'])

        self.lap, self.api = lap, kwargs['api']
        classification: str = None
        distance: float = getattr(self.lap, 'distance')
        track_length: float = getattr(self.lap, '_trackGpsLength')
        lapId: str = getattr(self.lap, 'lapName')
        # getting the max accelerator combine from api get away
        max_acc_comb: float = self.get_max_acc_comb(lapId=lapId,
                                                    user_id=getattr(self.lap, 'UserId'),
                                                    track_id=getattr(self.lap, 'TrackId'),
                                                    lap_start_date=getattr(self.lap, 'lapStartDate'),
                                                    api=self.api)
        setattr(self.lap, 'max_acc_comb', max_acc_comb)  # set the retrieved the max accelerator
        #  combine to lap object

        if self.lap.FULL_LAP_FLOOR * track_length <= distance < self.lap.FULL_LAP_FLOOR * track_length:
            classification = self._classify_full_lap()

        # Partial laps
        elif lap.PART_LAP_FLOOR * track_length < distance < lap.FULL_LAP_FLOOR * track_length and self._is_partial_lap():
            classification = 'Partial'

        # Non legit laps
        else:
            classification = 'NonLegit'

        return classification

    """
    @:function _classify_full_lap
    classifies the lap to competitive or non competitive according to the
    avg performance of the driver in this particular lap compared with the
    best performance in the lap's track
    """

    def _classify_full_lap(self) -> str:
        max_acc_comb, acc_comb = self.lap.max_acc_comb, getattr(self.lap, 'accCombinedAvg')
        classification: str = None
        try:
            if max_acc_comb is None:
                # No accCombined data yet for this
                classification = "Competitive"
            else:
                # set the lap to competitive based on the combined accelerations of the lap (per Track, per driver)
                if acc_comb > self.lap.MAX_ACC_PERCENT * max_acc_comb and self.lap._low_speed_time < 1:
                    classification = "Competitive"
                elif acc_comb > self.lap.MAX_ACC_PERCENT * max_acc_comb and 1 <= self.lap._low_speed_time <= 30:
                    classification = "Non-Successful"
                else:
                    classification = 'NonCompetitive'
        except Exception as e:
            print(f" Exception raised in class RuleBaseClassifier._classify_full_lap"
                  f" {e}")
        finally:
            return classification

    """
    @:function _is_partial_lap
    @:returns True if is partial lap False if not
    """

    def _is_partial_lap(self) -> bool:
        acc_comb, max_acc_comb = getattr(self.lap, 'accCombinedAvg'), getattr(self.lap, 'max_acc_comb')
        return max_acc_comb is None or acc_comb > self.lap.MAX_ACC_PERCENT * max_acc_comb

    """
    @:function get_max_acc_comb  
    @:params:
    @:lapId = the processed lap id 
    @: user_id  = the lap user id 
    @: track_id = the track where the lap is running
    @: lap_start_date = the lap start date
    @: api = a class white a get function that get the above params and return the 
    max accelerator combine (api is some api for calculation now is the api get away )
    
    @:returns the max accelerator combine of this lap 
    """

    @staticmethod
    def get_max_acc_comb(*, lapId, user_id, track_id, lap_start_date, api) -> float:
        acc_comb: float = 0.0
        payload = {'userId': user_id, 'trackId': track_id, 'startDate': lap_start_date}
        try:
            result = api.get("/driverlaps/max_acc_comb/", params=payload).json()
            if result['maxAcc'] is not None:
                acc_comb = round(float(result['maxAcc']), 3)
            else:
                acc_comb = None

        except Exception as e:
            print(f"in lap {lapId} exception raised in class classify function get_max_acc_comb {e}")
            acc_comb = None

        finally:
            return acc_comb


ruleBaseClassifier = RuleBaseClassifier()
