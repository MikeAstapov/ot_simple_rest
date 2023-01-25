import logging
import parsers.otl_resolver.Resolver as Resolver
from utils.primitives import EverythingEqual

OTL = """| makeresults | eval rmt = mvappend(1,2,7,3), b ="A" | mvexpand rmt | join rmt type=inner [| makeresults | eval rmt = mvappend(8,9,5,4), t ="X" | mvexpand rmt | foreach *mt [| eval c = sqrt(<<FIELD>>)]]"""


def main():
    resolver1 = Resolver.Resolver([EverythingEqual(), 'main', 'main1', 'main2'], 0, 0,
                                  no_subsearch_commands='foreach,appendpipe', macros_dir='./tests/macros/')
    resolver2 = Resolver.Resolver([EverythingEqual(), 'main', 'main1', 'main2'], 0, 0,
                                  no_subsearch_commands='foreach,appendpipe', macros_dir='./tests/macros/')
    logging.basicConfig(
        level='DEBUG',
        format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
        func=%(funcName)s - %(message)s")

    print("calculating OTL")
    correct_result = resolver2.resolve(OTL)
    print('result', correct_result)


if __name__ == "__main__":
    main()
