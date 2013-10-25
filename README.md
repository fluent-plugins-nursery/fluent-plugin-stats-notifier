# fluent-plugin-calc-notifier [![Build Status](https://secure.travis-ci.org/sonots/fluent-plugin-calc-notifier.png?branch=master)](http://travis-ci.org/sonots/fluent-plugin-calc-notifier) [![Dependency Status](https://gemnasium.com/sonots/fluent-plugin-calc-notifier.png)](https://gemnasium.com/sonots/fluent-plugin-calc-notifier)

Fluentd plugin to aggregate count messages with calculation and then thresholding

## Configuration

  <store>
    type calc_notifier
    tag notifier
    interval 5
    target_key 4xx_count
    greater_equal 4
    compare_with max
    store_file /path/to/store_file.dat
  </store>

Assuming following inputs are coming:

    foo.bar1: {"4xx_count":1,"foobar":2"}
    foo.bar2: {"4xx_count":6,"foobar":2"}

then this plugin emits an message because the max of `4xx_count` is greater than or equal to the specified value `4`. Output will be as following:

    notifier: {"4xx_count":6.0}

## Parameters

- target\_key (required)

    The target key in the event record.

- interval

    The interval time of calculation and bounding. Default is 60.

- less\_than

    A `less than` threshold value, that is, emit if `target_key` value < specified value.

- less\_equal

    A `less than or eqaul` threshold value, that is, emit if `target_key` value <= specified value.

- greater\_than

    A `greater than` threshold value, that is, emit if `target_key` value > specified value. 

- greater\_equal

    A `greater than or eqaul` threshold value, that is, emit if `target_key` value >= specified value. 

- compare\_with

    `max`, `avg`, `min`, `sum` can be specified. Default is `max`.

- tag

    The output tag name. 

- add_tag_prefix

    (not available yet) Add tag prefix for output message. 

- aggragate
    
    (not available yet) Do calculation by each `tag` or `all`. The default value is `tag`.

- store_file

    Store internal data into a file of the given path on shutdown, and load on starting.

## ChangeLog

See [CHANGELOG.md](CHANGELOG.md) for details.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new [Pull Request](../../pull/new/master)

## Copyright

Copyright (c) 2013 Naotoshi SEO. See [LICENSE](LICENSE) for details.

