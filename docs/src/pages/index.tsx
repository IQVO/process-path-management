import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import HomepageFeatures from '@site/src/components/HomepageFeatures';
import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns (WMS/WES/WCS, CloudEvents,
      RFC 7807, hexagonal architecture). Not a production system. Not
      affiliated with, endorsed by, or representative of Manhattan
      Associates, Blue Yonder, or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · Generic subdomain · published-language source
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          The single, operator-configurable source of process-path
          definitions — replacing a static YAML catalogue previously
          boot-loaded by three other services. Publishes every change onto
          Kafka; never a synchronous dependency for any consumer.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/overview">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference/rest/process-path-management-api">
            API Reference
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/adr">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Process Path Management bounded context: the operator-configurable process-path catalogue for the warehouse-systems fleet.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              This service is the SOURCE of the process-path published
              language. It is never a consumer of anyone else's.
            </blockquote>
            <p className={styles.invariantCaption}>
              Downstream contexts learn about a path change exclusively via
              Kafka — no synchronous callback into this service exists or is
              planned.{' '}
              <Link to="/docs/ecosystem/context-map">
                See the context map →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}
