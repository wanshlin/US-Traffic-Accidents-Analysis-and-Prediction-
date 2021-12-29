import Head from "next/head";
import Navigation from "./share/navigation";
import Content from "./address/content";
export default function Home() {
  return (
    <div className="flex flex-col min-h-screen font-sans bg-blue-100">
      <Head>
        <title>Accident Analysis</title>
      </Head>
      <Navigation />
      <Content />
    </div>
  );
}
