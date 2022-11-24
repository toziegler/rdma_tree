#pragma once
#include <stdexcept>
#include <random>
#include "RandomGenerator.hpp"
/**
 * Zipf (Zeta) random distribution.
 *
 * Implementation taken from drobilla's May 24, 2017 answer to
 * https://stackoverflow.com/questions/9983239/how-to-generate-zipf-distributed-numbers-efficiently
 *
 * That code is referenced with this:
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 *
 * Note that the Hörmann & Derflinger paper, and the stackoverflow
 * code base incorrectly names the paramater as `q`, when they mean `s`.
 * Thier `q` has nothing to do with the q-series. The names in the code
 * below conform to conventions.
 *
 * Example usage:
 *
 *    std::random_device rd;
 *    std::mt19937 gen(rd());
 *    zipf_distribution<> zipf(300);
 *
 *    for (int i = 0; i < 100; i++)
 *        printf("draw %d %d\n", i, zipf(gen));
 */

template<class IntType = unsigned long, class RealType = double>
class zipf_distribution
{
	// public:
	// 	typedef IntType result_type;

	// 	static_assert(std::numeric_limits<IntType>::is_integer, "");
	// 	static_assert(!std::numeric_limits<RealType>::is_integer, "");

	// 	/// zipf_distribution(N, s, q)
	// 	/// Zipf distribution for `N` items, in the range `[1,N]` inclusive.
	// 	/// The distribution follows the power-law 1/(n+q)^s with exponent
	// 	/// `s` and Hurwicz q-deformation `q`.
	// 	zipf_distribution(const IntType n=std::numeric_limits<IntType>::max(),
	// 	                  const RealType s=1.0,
	// 	                  const RealType q=0.0)
	// 		: n(n)
	// 		, _s(s)
	// 		, _q(q)
	// 		, oms(1.0-s)
	// 		, spole(abs(oms) < epsilon)
	// 		, rvs(spole ? 0.0 : 1.0/oms)
	// 		, H_x1(H(1.5) - h(1.0))
	// 		, H_n(H(n + 0.5))
	// 		, cut(1.0 - H_inv(H(1.5) - h(1.0)))
	// 		, dist(H_x1, H_n)
	// 	{
	// 		if (-0.5 >= q)
	// 			throw std::runtime_error("Range error: Parameter q must be greater than -0.5!");
	// 	}
	// 	void reset() {}

	// 	IntType operator()(std::mt19937& rng)
	// 	{
	// 		while (true)
	// 		{
	// 			const RealType u = dist(rng);
	// 			const RealType x = H_inv(u);
	// 			const IntType  k = std::round(x);
	// 			if (k - x <= cut) return k;
	// 			if (u >= H(k + 0.5) - h(k))
	// 				return k;
	// 		}
	// 	}

	// 	/// Returns the parameter the distribution was constructed with.
	// 	RealType s() const { return _s; }
	// 	/// Returns the Hurwicz q-deformation parameter.
	// 	RealType q() const { return _q; }
	// 	/// Returns the minimum value potentially generated by the distribution.
	// 	result_type min() const { return 1; }
	// 	/// Returns the maximum value potentially generated by the distribution.
	// 	result_type max() const { return n; }


	// private:
	// 	IntType    n;     ///< Number of elements
	// 	RealType   _s;    ///< Exponent
	// 	RealType   _q;    ///< Deformation
	// 	RealType   oms;   ///< 1-s
	// 	bool       spole; ///< true if s near 1.0
	// 	RealType   rvs;   ///< 1/(1-s)
	// 	RealType   H_x1;  ///< H(x_1)
	// 	RealType   H_n;   ///< H(n)
	// 	RealType   cut;   ///< rejection cut
	// 	std::uniform_real_distribution<RealType> dist;  ///< [H(x_1), H(n)]

	// 	// This provides 16 decimal places of precision,
	// 	// i.e. good to (epsilon)^4 / 24 per expanions log, exp below.
	// 	static constexpr RealType epsilon = 2e-5;

	// 	/** (exp(x) - 1) / x */
	// 	static double
	// 	expxm1bx(const double x)
	// 	{
	// 		if (std::abs(x) > epsilon)
	// 			return std::expm1(x) / x;
	// 		return (1.0 + x/2.0 * (1.0 + x/3.0 * (1.0 + x/4.0)));
	// 	}

	// 	/** log(1 + x) / x */
	// 	static RealType
	// 	log1pxbx(const RealType x)
	// 	{
	// 		if (std::abs(x) > epsilon)
	// 			return std::log1p(x) / x;
	// 		return 1.0 - x * ((1/2.0) - x * ((1/3.0) - x * (1/4.0)));
	// 	}

	// 	/**
	// 	 * The hat function h(x) = 1/(x+q)^s
	// 	 */
	// 	const RealType h(const RealType x)
	// 	{
	// 		return std::pow(x + _q, -_s);
	// 	}

	// 	/**
	// 	 * H(x) is an integral of h(x).
	// 	 *     H(x) = [(x+q)^(1-s) - (1+q)^(1-s)] / (1-s)
	// 	 * and if s==1 then
	// 	 *     H(x) = log(x+q) - log(1+q)
	// 	 *
	// 	 * Note that the numerator is one less than in the paper
	// 	 * order to work with all s. Unfortunately, the naive
	// 	 * implementation of the above hits numerical underflow
	// 	 * when q is larger than 10 or so, so we split into
	// 	 * different regimes.
	// 	 *
	// 	 * When q != 0, we shift back to what the paper defined:
	// 	 *    H(x) = (x+q)^{1-s} / (1-s)
	// 	 * and for q != 0 and also s==1, use
	// 	 *    H(x) = [exp{(1-s) log(x+q)} - 1] / (1-s)
	// 	 */
	// 	const RealType H(const RealType x)
	// 	{
	// 		if (not spole)
	// 			return std::pow(x + _q, oms) / oms;

	// 		const RealType log_xpq = std::log(x + _q);
	// 		return log_xpq * expxm1bx(oms * log_xpq);
	// 	}

	// 	/**
	// 	 * The inverse function of H(x).
	// 	 *    H^{-1}(y) = [(1-s)y + (1+q)^{1-s}]^{1/(1-s)} - q
	// 	 * Same convergence issues as above; two regimes.
	// 	 *
	// 	 * For s far away from 1.0 use the paper version
	// 	 *    H^{-1}(y) = -q + (y(1-s))^{1/(1-s)}
	// 	 */
	// 	const RealType H_inv(const RealType y)
	// 	{
	// 		if (not spole)
	// 			return std::pow(y * oms, rvs) - _q;

	// 		return std::exp(y * log1pxbx(oms * y)) - _q;
	// 	}
   	public:
		typedef IntType result_type;

		static_assert(std::numeric_limits<IntType>::is_integer, "");
		static_assert(!std::numeric_limits<RealType>::is_integer, "");

		/// zipf_distribution(N, s, q)
		/// Zipf distribution for `N` items, in the range `[1,N]` inclusive.
		/// The distribution follows the power-law 1/(n+q)^s with exponent
		/// `s` and Hurwicz q-deformation `q`.
		zipf_distribution(const IntType n=std::numeric_limits<IntType>::max(),
		                  const RealType s=1.0,
		                  const RealType q=0.0)
			: n(n)
			, _s(s)
			, _q(q)
			, oms(1.0-s)
			, spole(abs(oms) < epsilon)
			, rvs(spole ? 0.0 : 1.0/oms)
			, H_x1(H(1.5) - h(1.0))
			, H_n(H(n + 0.5))
			, cut(1.0 - H_inv(H(1.5) - h(1.0)))
		{
			if (-0.5 >= q)
				throw std::runtime_error("Range error: Parameter q must be greater than -0.5!");
		}
		void reset() {}

		IntType operator()([[maybe_unused]] std::mt19937& rng)
		{
			while (true)
			{

               auto randomValue = random_real();
               // double randomValue = ((double) rand() / (RAND_MAX));
               const RealType u =H_x1 + randomValue *( H_n - H_x1 );
               const RealType x = H_inv(u);
               const IntType  k = std::round(x);
               if (k - x <= cut) return k;
               if (u >= H(k + 0.5) - h(k))
                  return k;
			}
		}

		/// Returns the parameter the distribution was constructed with.
		RealType s() const { return _s; }
		/// Returns the Hurwicz q-deformation parameter.
		RealType q() const { return _q; }
		/// Returns the minimum value potentially generated by the distribution.
		result_type min() const { return 1; }
		/// Returns the maximum value potentially generated by the distribution.
		result_type max() const { return n; }


  private:
   IntType    n;     ///< Number of elements
   RealType   _s;    ///< Exponent
   RealType   _q;    ///< Deformation
   RealType   oms;   ///< 1-s
   bool       spole; ///< true if s near 1.0
   RealType   rvs;   ///< 1/(1-s)
   RealType   H_x1;  ///< H(x_1)
   RealType   H_n;   ///< H(n)
   RealType   cut;   ///< rejection cut

   // This provides 16 decimal places of precision,
   // i.e. good to (epsilon)^4 / 24 per expanions log, exp below.
   static constexpr RealType epsilon = 2e-5;

   /** (exp(x) - 1) / x */
   static double
   expxm1bx(const double x)
      {
         if (std::abs(x) > epsilon)
            return std::expm1(x) / x;
         return (1.0 + x/2.0 * (1.0 + x/3.0 * (1.0 + x/4.0)));
      }

   /** log(1 + x) / x */
   static RealType
   log1pxbx(const RealType x)
      {
         if (std::abs(x) > epsilon)
            return std::log1p(x) / x;
         return 1.0 - x * ((1/2.0) - x * ((1/3.0) - x * (1/4.0)));
      }


   uint64_t	random64(void){
      return farm::utils::RandomGenerator::getRandU64();
   }

/*
 * random_real_64: Pick an integer in {0, 1, ..., 2^64 - 1} uniformly
 * at random, convert it to double, and divide it by 2^64.  Values in
 * [2^-11, 1] are overrepresented, small exponents have low precision,
 * and exponents below -64 are not possible.
 */
   double
   random_real_64(void)
      {

         return ldexp((double)random64(), -64);
      }

/*
 * random_real_53: Pick an integer in {0, 1, ..., 2^53 - 1} uniformly
 * at random, convert it to double, and divide it by 2^53.  Many
 * possible outputs are not represented: 2^-54, 1, &c.  There are a
 * little under 2^62 floating-point values in [0, 1], but only 2^53
 * possible outputs here.
 */
   double
   random_real_53(void)
      {

         return ldexp((double)(random64() & ((1ULL << 53) - 1)), -53);
      }

#define	clz64	__builtin_clzll		/* XXX GCCism */

/*
 * random_real: Generate a stream of bits uniformly at random and
 * interpret it as the fractional part of the binary expansion of a
 * number in [0, 1], 0.00001010011111010100...; then round it.
 */
   double
   random_real(void)
      {
         int exponent = -64;
         uint64_t significand;
         unsigned shift;

         /*
          * Read zeros into the exponent until we hit a one; the rest
          * will go into the significand.
          */
         while (((significand = random64()) == 0)) {
            exponent -= 64;
            /*
             * If the exponent falls below -1074 = emin + 1 - p,
             * the exponent of the smallest subnormal, we are
             * guaranteed the result will be rounded to zero.  This
             * case is so unlikely it will happen in realistic
             * terms only if random64 is broken.
             */
            if ((exponent < -1074))
               return 0;
         }

         /*
          * There is a 1 somewhere in significand, not necessarily in
          * the most significant position.  If there are leading zeros,
          * shift them into the exponent and refill the less-significant
          * bits of the significand.  Can't predict one way or another
          * whether there are leading zeros: there's a fifty-fifty
          * chance, if random64 is uniformly distributed.
          */
         shift = clz64(significand);
         if (shift != 0) {
            exponent -= shift;
            significand <<= shift;
            significand |= (random64() >> (64 - shift));
         }

         /*
          * Set the sticky bit, since there is almost surely another 1
          * in the bit stream.  Otherwise, we might round what looks
          * like a tie to even when, almost surely, were we to look
          * further in the bit stream, there would be a 1 breaking the
          * tie.
          */
         significand |= 1;

         /*
          * Finally, convert to double (rounding) and scale by
          * 2^exponent.
          */
         return ldexp((double)significand, exponent);
      }

   /**
    * The hat function h(x) = 1/(x+q)^s
    */
   const RealType h(const RealType x)
      {
         return std::pow(x + _q, -_s);
      }

   /**
    * H(x) is an integral of h(x).
    *     H(x) = [(x+q)^(1-s) - (1+q)^(1-s)] / (1-s)
    * and if s==1 then
    *     H(x) = log(x+q) - log(1+q)
    *
    * Note that the numerator is one less than in the paper
    * order to work with all s. Unfortunately, the naive
    * implementation of the above hits numerical underflow
    * when q is larger than 10 or so, so we split into
    * different regimes.
    *
    * When q != 0, we shift back to what the paper defined:
    *    H(x) = (x+q)^{1-s} / (1-s)
    * and for q != 0 and also s==1, use
    *    H(x) = [exp{(1-s) log(x+q)} - 1] / (1-s)
    */
   const RealType H(const RealType x)
      {
         if (not spole)
            return std::pow(x + _q, oms) / oms;

         const RealType log_xpq = std::log(x + _q);
         return log_xpq * expxm1bx(oms * log_xpq);
      }

   /**
    * The inverse function of H(x).
    *    H^{-1}(y) = [(1-s)y + (1+q)^{1-s}]^{1/(1-s)} - q
    * Same convergence issues as above; two regimes.
    *
    * For s far away from 1.0 use the paper version
    *    H^{-1}(y) = -q + (y(1-s))^{1/(1-s)}
    */
   const RealType H_inv(const RealType y)
      {
         if (not spole)
            return std::pow(y * oms, rvs) - _q;

         return std::exp(y * log1pxbx(oms * y)) - _q;
      }
};

/**
 * Same API as above, but about 25% faster for N=30,and 10% faster for
 * N=300, and tied with above for N=1000.
 *
 * This has a much slower initialization, because of the std::pow()
 * function, and also this will thrash the d-cache for N much greater
 * than N=1000 (because this requires lookup in the std::vector<> array).
 * Results will vary depending on your memory subystem performance.
 */
template<class IntType = unsigned long, class RealType = double>
class zipf_table_distribution
{
	public:
		typedef IntType result_type;

		static_assert(std::numeric_limits<IntType>::is_integer, "");
		static_assert(!std::numeric_limits<RealType>::is_integer, "");

		/// zipf_table_distribution(N, s)
		/// Zipf distribution for `N` items, in the range `[1,N]` inclusive.
		/// The distribution follows the power-law 1/n^s with exponent `s`.
		/// This uses a table-lookup, and thus provides values more
		/// quickly than zipf_distribution. However, the table can take
		/// up a considerable amount of RAM, and initializing this table
		/// can consume significant time.
		zipf_table_distribution(const IntType n,
		                        const RealType s=1.0,
		                        const RealType q=0.0) :
			_n(init(n,s,q)),
			_s(s),
			_q(q),
			_dist(_pdf.begin(), _pdf.end())
		{}
		void reset() {}

		IntType operator()(std::mt19937& rng)
		{
			return _dist(rng);
		}

		/// Returns the parameter the distribution was constructed with.
		RealType s() const { return _s; }

		/// Returns the Hurwicz q parameter.
		RealType q() const { return _q; }

		/// Returns the minimum value potentially generated by the distribution.
		result_type min() const { return 1; }
		/// Returns the maximum value potentially generated by the distribution.
		result_type max() const { return _n; }

	private:
		std::vector<RealType>               _pdf;  ///< Prob. distribution
		IntType                             _n;    ///< Number of elements
		RealType                            _s;    ///< Exponent
		RealType                            _q;    ///< Hurwicz q
		std::discrete_distribution<IntType> _dist; ///< Draw generator

		/** Initialize the probability mass function */
		IntType init(const IntType n, const RealType s, const RealType q)
		{
			_pdf.reserve(n+1);
			_pdf.emplace_back(0.0);
			for (IntType i=1; i<=n; i++)
				_pdf.emplace_back(std::pow(q + (double)i, -s));
			return n;
		}
};
