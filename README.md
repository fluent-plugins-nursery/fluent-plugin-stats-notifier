# fluent-plugin-stats-notifier [![Build Status](https://secure.travis-ci.org/sonots/fluent-plugin-stats-notifier.png?branch=master)](http://travis-ci.org/sonots/fluent-plugin-stats-notifier)

Fluentd plugin to calculate statistics and then thresholding

## Configuration

### Example 1

Get statistics on messages

```
<match foo.**>
  type stats_notifier
  tag notifier
  interval 5
  target_key 4xx_count
  greater_equal 4
  stats max # default
  store_file /path/to/store_file.dat
</match>
```

Assuming following inputs are coming:

    foo.bar1: {"4xx_count":1,"foobar":2"}
    foo.bar1: {"4xx_count":6,"foobar":2"}

then this plugin emits an message because the max of `4xx_count` is greater than or equal to the specified value `4`. Output will be as following:

    notifier: {"4xx_count":6.0}

### Example 2

Get statistics among tags

```
<match foo.**>
  type stats_notifier
  tag notifier
  interval 5
  target_key 4xx_count
  greater_equal 4
  aggregate all # default
  aggregate_stats max # default
  store_file /path/to/store_file.dat
</match>
```

Assuming following inputs are coming:

    foo.bar1: {"4xx_count":1,"foobar":2"}
    foo.bar2: {"4xx_count":6,"foobar":2"}

then this plugin emits an message because the max of `4xx_count` is greater than or equal to the specified value `4`. Output will be as following:

    notifier: {"4xx_count":6.0}

### Combined Example

```
<match foo.**>
  type stats_notifier
  tag notifier
  interval 5
  target_key 4xx_count
  greater_equal 4
  stats max # default
  aggregate all # default
  aggregate_stats max # default
  store_file /path/to/store_file.dat
</match>
```

Assuming following inputs are coming:

    foo.bar1: {"4xx_count":1,"foobar":2"}
    foo.bar1: {"4xx_count":8,"foobar":2"}
    foo.bar2: {"4xx_count":6,"foobar":2"}

Output will be as following:

    notifier: {"4xx_count":8.0}

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

- stats

    `max`, `avg`, `min`, `sum` can be specified. Default is `max`.

- aggregate\_stats

    Work only with `aggregate all`. `max`, `avg`, `min`, `sum` can be specified. Default is `max`.

- compare\_with 

    Obsolete. Use `aggregate_stats`.

- tag

    The output tag name. Required for `aggregate all`.

- add_tag_prefix

    Add tag prefix for output message. Required for `aggregate tag`.

- remove_tag_prefix

    Remove tag prefix for output message.

- add_tag_suffix

    Add tag suffix for output message.

- remove_tag_suffix

    Remove tag suffix for output message.

- aggragate
    
    Do calculation for each `tag` or `all`. The defaultis `all`.

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

Copyright (c) 2013 Naotoshi Seo. See [LICENSE](LICENSE) for details.

