use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use prometheus::{
    core::{
        Atomic, AtomicF64, AtomicI64, AtomicU64, Collector, GenericCounter, GenericCounterVec,
        GenericGauge, GenericGaugeVec,
    },
    CounterVec, Encoder, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounterVec,
    IntGaugeVec, Opts, Registry,
};
use tokio::task::JoinHandle;

use warp::Filter;

#[derive(Debug, PartialEq, Eq, Hash)]
/// A metric descriptor with static string refs, suitable for compile-time
/// instantiation. `MetricDescriptor` is generic over the type of Metric that
/// it describes. It is used to instantiate and register (or retrieve) a metric
/// vec which produces that metric.
///
/// Generally it is recommended that users use the type aliases rather than
/// this type:
/// - [`IntCounterVecDescriptor`]
/// - [`CounterVecDescriptor`]
/// - [`IntGaugeVecDescriptor`]
/// - [`GaugeVecDescriptor`]
/// - [`HistogramVecDescriptor`]
///
/// Descriptors are intended to be instantiated at compile time via `const fn
/// new`.
///
/// ```
/// use sisyphus::metrics::HistogramVecDescriptor;
/// const COOL_METRIC: HistogramVecDescriptor<4> = HistogramVecDescriptor::new(
///     Some("subsystem"),
///     "name",
///     "help text",
///     ["1","2","3","4"] // label names
/// );
/// ```
pub struct MetricDescriptor<T, const N: usize>
where
    T: Collector,
{
    /// Subsystem Name (prepended to metric name in fully qualified name)
    subsystem: Option<&'static str>,
    /// Metric Name
    name: &'static str,
    /// Help Text
    help: &'static str,
    /// Label names
    labels: [&'static str; N],
    /// spoooooky :o
    _phantom: PhantomData<*const T>,
}

impl<T, const N: usize> MetricDescriptor<T, N>
where
    T: Collector,
{
    /// Instantiate a metric descriptor
    pub const fn new(
        subsystem: Option<&'static str>,
        name: &'static str,
        help: &'static str,
        labels: [&'static str; N],
    ) -> Self {
        Self {
            subsystem,
            name,
            help,
            labels,
            _phantom: PhantomData,
        }
    }

    /// Qualified name is subsystem and name joined by `_`, or simply the name
    /// if subsystem is `None`
    pub fn qualified_name(&self) -> String {
        if let Some(subsystem) = self.subsystem {
            [subsystem, self.name].join("_")
        } else {
            self.name.to_owned()
        }
    }

    /// Metric name
    pub const fn name(&self) -> &str {
        self.name
    }

    /// Metric subsystem name
    pub const fn subsystem(&self) -> Option<&str> {
        self.subsystem
    }

    /// Metric help text
    pub const fn help(&self) -> &str {
        self.help
    }

    /// Metric label names
    pub const fn label_names(&self) -> [&str; N] {
        self.labels
    }
}

impl<T, const N: usize> Clone for MetricDescriptor<T, N>
where
    T: Collector,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            subsystem: self.subsystem,
            help: self.help,
            labels: self.labels,
            _phantom: self._phantom,
        }
    }
}

impl<T, const N: usize> Copy for MetricDescriptor<T, N> where T: Collector {}

impl<T, const N: usize> From<&MetricDescriptor<T, N>> for Opts
where
    T: Collector,
{
    fn from(m: &MetricDescriptor<T, N>) -> Self {
        let mut o = Opts::new(m.name, m.help);
        if let Some(subsytem) = m.subsystem() {
            o = o.subsystem(subsytem);
        }
        o
    }
}

impl<T, const N: usize> From<&MetricDescriptor<T, N>> for HistogramOpts
where
    T: Collector,
{
    fn from(m: &MetricDescriptor<T, N>) -> Self {
        Opts::from(m).into()
    }
}

/// A `MetricHandle` is a handle to a metric family registered on a specific
/// core. It is used to create a new metric
pub trait MetricHandle<const N: usize>: Sized {
    /// The type of metric in the vector
    type Metric: Collector;

    /// A reference to the core on which this metric vector is registered
    fn core(&self) -> &Metrics;

    /// Return a copy of the metric descriptor for this metric
    fn descriptor(&self) -> MetricDescriptor<Self::Metric, N>;

    /// Return the fully-qualified name of this metric
    fn full_name(&self) -> String {
        return self.core().full_name(&self.descriptor());
    }

    /// Get a metric with the specified label values. This will return an
    /// existing metric if one has been created with the provided labels, or
    /// create a new one
    fn metric(&self, label_values: [&str; N]) -> Self::Metric;
}

#[derive(Debug, Clone)]
/// Handle to a registered gauge vec
pub struct Gvh<'a, T, const N: usize>
where
    T: Atomic,
{
    /// A reference to the metrics on which this is registered
    core: &'a Metrics,
    descriptor: MetricDescriptor<<Self as MetricHandle<N>>::Metric, N>,
    vec: GenericGaugeVec<T>,
}

impl<'a, T, const N: usize> MetricHandle<N> for Gvh<'a, T, N>
where
    T: Atomic,
{
    type Metric = GenericGauge<T>;

    fn core(&self) -> &Metrics {
        self.core
    }

    fn metric(&self, label_values: [&str; N]) -> Self::Metric {
        self.vec
            .get_metric_with_label_values(label_values.as_ref())
            .expect("enforced by type system")
    }

    fn descriptor(&self) -> MetricDescriptor<<Self as MetricHandle<N>>::Metric, N> {
        self.descriptor
    }
}

#[derive(Debug, Clone)]
/// Handle to a registered counter vec
pub struct Cvh<'a, T, const N: usize>
where
    T: Atomic,
{
    /// A reference to the metrics on which this is registered
    core: &'a Metrics,
    descriptor: MetricDescriptor<<Self as MetricHandle<N>>::Metric, N>,
    vec: GenericCounterVec<T>,
}

impl<'a, T, const N: usize> MetricHandle<N> for Cvh<'a, T, N>
where
    T: Atomic,
{
    type Metric = GenericCounter<T>;

    fn core(&self) -> &Metrics {
        self.core
    }

    fn descriptor(&self) -> MetricDescriptor<<Self as MetricHandle<N>>::Metric, N> {
        self.descriptor
    }

    fn metric(&self, label_values: [&str; N]) -> Self::Metric {
        self.vec
            .get_metric_with_label_values(label_values.as_ref())
            .expect("enforced by type system")
    }
}

/// Handle to a registered histogram vec
pub struct HistogramVecHandle<'a, const N: usize> {
    /// A reference to the metrics on which this is registered
    core: &'a Metrics,
    descriptor: MetricDescriptor<<Self as MetricHandle<N>>::Metric, N>,
    buckets: Vec<f64>,
    vec: HistogramVec,
}

impl<'a, const N: usize> HistogramVecHandle<'a, N> {
    /// Return the buckets for this histogram
    pub fn buckets(&self) -> &[f64] {
        self.buckets.as_ref()
    }
}

impl<'a, const N: usize> MetricHandle<N> for HistogramVecHandle<'a, N> {
    type Metric = Histogram;

    fn core(&self) -> &Metrics {
        self.core
    }

    fn descriptor(&self) -> MetricDescriptor<<Self as MetricHandle<N>>::Metric, N> {
        self.descriptor
    }

    fn metric(&self, label_values: [&str; N]) -> Self::Metric {
        self.vec
            .get_metric_with_label_values(label_values.as_ref())
            .expect("enforced by type system")
    }
}

/// Descriptor for an IntCounterVec
pub type IntCounterVecDescriptor<'a, const N: usize> =
    MetricDescriptor<<IntCounterVecHandle<'a, N> as MetricHandle<N>>::Metric, N>;
/// Descriptor for an CounterVec
pub type CounterVecDescriptor<'a, const N: usize> =
    MetricDescriptor<<CounterVecHandle<'a, N> as MetricHandle<N>>::Metric, N>;
/// Descriptor for an IntGaugeVec
pub type IntGaugeVecDescriptor<'a, const N: usize> =
    MetricDescriptor<<IntGaugeVecHandle<'a, N> as MetricHandle<N>>::Metric, N>;
/// Descriptor for an GaugeVec
pub type GaugeVecDescriptor<'a, const N: usize> =
    MetricDescriptor<<GaugeVecHandle<'a, N> as MetricHandle<N>>::Metric, N>;
/// Descriptor for a HistogramVec
pub type HistogramVecDescriptor<'a, const N: usize> =
    MetricDescriptor<<HistogramVecHandle<'a, N> as MetricHandle<N>>::Metric, N>;

/// Handle to an IntCounterVec
pub type IntCounterVecHandle<'a, const N: usize> = Cvh<'a, AtomicU64, N>;
/// Handle to an CounterVec
pub type CounterVecHandle<'a, const N: usize> = Cvh<'a, AtomicF64, N>;
/// Handle to an IntGaugeVec
pub type IntGaugeVecHandle<'a, const N: usize> = Gvh<'a, AtomicI64, N>;
/// Handle to an GaugeVec
pub type GaugeVecHandle<'a, const N: usize> = Gvh<'a, AtomicF64, N>;

macro_rules! register {
    ($self:ident, $metric:ident) => {
        $self
            .registry
            .register(Box::new($metric.clone()))
            .expect("registry broken");
    };
}

macro_rules! get_or_insert {
    ($self:ident, $map:ident, $metric_vec:ty, $descriptor:ident $(,)?) => {{
        $self
            .$map
            .lock()
            .expect("poison")
            .entry($descriptor.name)
            .or_insert_with(|| {
                let metric = <$metric_vec>::new(
                    $self.opts($descriptor.name, $descriptor.help),
                    $descriptor.labels.as_ref(),
                )
                .expect("invalid name, help, or labels");
                register!($self, metric);
                metric
            })
            .clone()
    }};
}

///  Metrics that can be registered on the fly
#[derive(Debug, Default)]
pub struct Metrics {
    registry: Registry,
    namespace: &'static str,
    icv: Mutex<HashMap<&'static str, IntCounterVec>>,
    cv: Mutex<HashMap<&'static str, CounterVec>>,
    igv: Mutex<HashMap<&'static str, IntGaugeVec>>,
    gv: Mutex<HashMap<&'static str, GaugeVec>>,
    hv: Mutex<HashMap<&'static str, HistogramVec>>,
}

impl Metrics {
    /// Resolve the fully qualified name of a metric based on its descriptor
    pub fn full_name<T, const N: usize>(&self, descriptor: &MetricDescriptor<T, N>) -> String
    where
        T: Collector,
    {
        [self.namespace, &descriptor.qualified_name()].join("_")
    }

    /// Generate an opts struct with the name and help
    pub fn opts(&self, name: &'static str, help: &'static str) -> Opts {
        Opts::new(name, help)
            .namespace(self.namespace)
            .const_label("PKG_VERSION", env!("CARGO_PKG_VERSION"))
    }

    /// Generate a histogram opts
    pub fn histogram_opts(
        &self,
        name: &'static str,
        help: &'static str,
        buckets: &[f64],
    ) -> HistogramOpts {
        HistogramOpts {
            common_opts: self.opts(name, help),
            buckets: buckets.to_vec(),
        }
    }

    /// IntCounterVec
    pub fn icv<'a, const N: usize>(
        &'a self,
        descriptor: IntCounterVecDescriptor<'a, N>,
    ) -> IntCounterVecHandle<'a, N> {
        let icv = get_or_insert!(self, icv, IntCounterVec, descriptor);
        IntCounterVecHandle {
            core: self,
            descriptor,
            vec: icv,
        }
    }

    /// CounterVec
    pub fn cv<'a, const N: usize>(
        &'a self,
        descriptor: CounterVecDescriptor<'a, N>,
    ) -> CounterVecHandle<'a, N> {
        let cv = get_or_insert!(self, cv, CounterVec, descriptor);
        CounterVecHandle {
            core: self,
            descriptor,
            vec: cv,
        }
    }

    /// GaugeVec
    pub fn igv<'a, const N: usize>(
        &'a self,
        descriptor: IntGaugeVecDescriptor<'a, N>,
    ) -> IntGaugeVecHandle<'a, N> {
        let igv = get_or_insert!(self, igv, IntGaugeVec, descriptor);
        IntGaugeVecHandle {
            core: self,
            descriptor,
            vec: igv,
        }
    }

    /// GaugeVec
    pub fn gv<'a, const N: usize>(
        &'a self,
        descriptor: GaugeVecDescriptor<'a, N>,
    ) -> GaugeVecHandle<'a, N> {
        let gv = get_or_insert!(self, gv, GaugeVec, descriptor);
        GaugeVecHandle {
            core: self,
            descriptor,
            vec: gv,
        }
    }

    /// HistogramVec
    pub fn hv<'a, const N: usize>(
        &'a self,
        descriptor: HistogramVecDescriptor<'a, N>,
        buckets: &[f64],
    ) -> HistogramVecHandle<'a, N> {
        let hv = self
            .hv
            .lock()
            .expect("poison")
            .entry(descriptor.name)
            .or_insert_with(|| {
                let metric = HistogramVec::new((&descriptor).into(), descriptor.labels.as_ref())
                    .expect("invalid name, help, or labels");
                register!(self, metric);
                metric
            })
            .clone();
        HistogramVecHandle {
            core: self,
            descriptor,
            buckets: buckets.to_vec(),
            vec: hv,
        }
    }

    /// Gather available metrics into an encoded (plaintext, OpenMetrics format) report.
    fn gather(&self) -> prometheus::Result<Vec<u8>> {
        let collected_metrics = self.registry.gather();
        let mut out_buf = Vec::with_capacity(1024 * 64);
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&collected_metrics, &mut out_buf)?;
        Ok(out_buf)
    }

    /// Serve metrics on a port
    pub fn serve(self: Arc<Self>, port: u16) -> JoinHandle<()> {
        tracing::info!(
            port,
            "starting prometheus server on 0.0.0.0:{port}",
            port = port
        );

        tokio::spawn(async move {
            warp::serve(
                warp::path!("metrics")
                    .map(move || {
                        warp::reply::with_header(
                            self.gather().expect("failed to encode metrics"),
                            "Content-Type",
                            // OpenMetrics specs demands "application/openmetrics-text; version=1.0.0; charset=utf-8"
                            // but the prometheus scraper itself doesn't seem to care?
                            // try text/plain to make web browsers happy.
                            "text/plain; charset=utf-8",
                        )
                    })
                    .or(warp::any().map(|| {
                        warp::http::Response::builder()
                            .header("Location", "/metrics")
                            .status(301)
                            .body("".to_string())
                    })),
            )
            .run(([0, 0, 0, 0], port))
            .await;
        })
    }
}
