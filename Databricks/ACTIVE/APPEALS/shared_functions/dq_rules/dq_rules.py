from abc import ABC, abstractmethod


class DQRulesBase(ABC):

    @abstractmethod
    def get_checks(self, checks={}):
        pass
